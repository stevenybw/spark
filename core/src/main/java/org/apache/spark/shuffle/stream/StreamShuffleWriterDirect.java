/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.ShuffleBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.*;

/**
 * This is a minimum demo to use executor-side merging to reduce the number of output files from O(M*R) to
 * O(R) comparing to the StreamShuffleWriterWithoutMerging.
 * @param <K>
 * @param <V>
 */
public class StreamShuffleWriterDirect<K, V> extends ShuffleWriter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(StreamShuffleWriterDirect.class);

  private static final ClassTag<Object> keyClassTag = ClassTag$.MODULE$.Object();
  private static final ClassTag<Object> valueClassTag = ClassTag$.MODULE$.Object();
  private boolean stopped = false;
  private BlockManager blockManager;
  private Partitioner partitioner;
  private int mapId;
  private SharedWriter sharedWriter;
  private BufExposedByteArrayOutputStream serBuf;
  private SerializationStream serStream;

  private static final class BufExposedByteArrayOutputStream extends ByteArrayOutputStream {
    BufExposedByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  StreamShuffleWriterDirect(
          BlockManager blockManager,
          StreamShuffleBlockResolver streamShuffleBlockResolver,
          StreamShuffleDirectHandle<K, V> handle,
          int mapId,
          TaskContext taskContext,
          SharedWriter sharedWriter,
          SparkConf conf) throws IOException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    ShuffleDependency<K, V, V> dep = handle.dependency();
    this.blockManager = blockManager;
    SerializerManager serializerManager = blockManager.serializerManager();
    SerializerInstance serializerInstance = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.mapId = mapId;
    this.sharedWriter = sharedWriter;
    this.serBuf = new BufExposedByteArrayOutputStream(4096);
    this.serStream = serializerInstance.serializeStream(serBuf);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    long numRecords = 0;
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final int pid = partitioner.getPartition(record._1());
      serBuf.reset();
      serStream.writeKey(record._1(), keyClassTag);
      serStream.writeValue(record._2(), valueClassTag);
      serStream.flush();
      int size = serBuf.size();
      byte[] buf = serBuf.getBuf();
      sharedWriter.append(pid, buf, size);
      numRecords++;
    }
    logger.info("Map " + mapId + "  NumRecords " + numRecords);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (!stopped) {
      stopped = true;
      if (success) {
        // return an empty MapStatus (because it has been merged)
        return Option.apply(MapStatus$.MODULE$.apply(blockManager.shuffleServerId()));
      } else {
        logger.info("stop unsuccessful shuffle writer");
        return None$.empty();
      }
    } else {
      logger.info("duplicated stop");
      return None$.empty();
    }
  }
}
