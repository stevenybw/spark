/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream;

import org.apache.spark.*;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
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
public class StreamShuffleWriterDirect<K, V, C> extends ShuffleWriter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(StreamShuffleWriterDirect.class);

  private static final ClassTag<Object> keyClassTag = ClassTag$.MODULE$.Object();
  private static final ClassTag<Object> valueClassTag = ClassTag$.MODULE$.Object();
  private static final ClassTag<Object> combinerClassTag = ClassTag$.MODULE$.Object();
  private final int shuffleId;
  private boolean stopped = false;
  private BlockManager blockManager;
  private Partitioner partitioner;
  private boolean mapSideCombine;
  private final Option<Aggregator<K, V, C>> aggregatorOpt;
  private int mapId;
  private FilesPartitionedWriter filesPartitionedWriter;
  private BufExposedByteArrayOutputStream serBuf;
  private SerializationStream serStream;
  private TaskContext taskContext;

  private static final class BufExposedByteArrayOutputStream extends ByteArrayOutputStream {
    BufExposedByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  StreamShuffleWriterDirect(
          BlockManager blockManager,
          StreamShuffleBlockResolver streamShuffleBlockResolver,
          StreamShuffleDirectHandle<K, V, C> handle,
          int mapId,
          TaskContext taskContext,
          FilesPartitionedWriter filesPartitionedWriter,
          SparkConf conf) throws IOException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    ShuffleDependency<K, V, C> dep = handle.dependency();
    this.shuffleId = handle.shuffleId();
    this.blockManager = blockManager;
    SerializerManager serializerManager = blockManager.serializerManager();
    SerializerInstance serializerInstance = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.mapSideCombine = dep.mapSideCombine();
    this.aggregatorOpt = dep.aggregator();
    this.mapId = mapId;
    this.filesPartitionedWriter = filesPartitionedWriter;
    this.serBuf = new BufExposedByteArrayOutputStream(4096);
    this.serStream = serializerInstance.serializeStream(serBuf);
    this.taskContext = taskContext;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    long recordsWritten = 0;
    long taskDuration = -System.nanoTime();
    long writeDuration = 0;
    long serializationDuration = 0;
    long bytesWritten = 0;
    if (!mapSideCombine) {
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final int pid = partitioner.getPartition(record._1());
        serializationDuration -= System.nanoTime();
        serBuf.reset();
        serStream.writeKey(record._1(), keyClassTag);
        serStream.writeValue(record._2(), valueClassTag);
        serStream.flush();
        serializationDuration += System.nanoTime();
        int size = serBuf.size();
        byte[] buf = serBuf.getBuf();
        writeDuration -= System.nanoTime();
        filesPartitionedWriter.append(pid, buf, size);
        writeDuration += System.nanoTime();
        recordsWritten++;
        bytesWritten+=size;
      }
    } else {
      Aggregator<K, V, C> aggregator = this.aggregatorOpt.get();
      Function1<V, C> createCombiner = aggregator.createCombiner();
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final int pid = partitioner.getPartition(record._1());
        serializationDuration -= System.nanoTime();
        serBuf.reset();
        serStream.writeKey(record._1(), keyClassTag);
        serStream.writeValue(createCombiner.apply(record._2()), combinerClassTag);
        serStream.flush();
        serializationDuration += System.nanoTime();
        int size = serBuf.size();
        byte[] buf = serBuf.getBuf();
        writeDuration -= System.nanoTime();
        filesPartitionedWriter.append(pid, buf, size);
        writeDuration += System.nanoTime();
        recordsWritten++;
        bytesWritten+=size;
      }
    }
    taskDuration += System.nanoTime();
    logger.info("YPerformanceMetric  map," + mapId +
            "," + shuffleId +
            "," + recordsWritten +
            "," + bytesWritten +
            "," + taskDuration +
            "," + writeDuration +
            "," + serializationDuration +
            "," + mapSideCombine);
    ShuffleWriteMetrics shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    shuffleWriteMetrics.incBytesWritten(bytesWritten);
    shuffleWriteMetrics.incRecordsWritten(recordsWritten);
    shuffleWriteMetrics.incWriteTime(writeDuration);
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
