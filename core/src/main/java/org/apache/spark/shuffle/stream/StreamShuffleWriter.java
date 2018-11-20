/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.stream.StreamShuffleBlockResolver;
import org.apache.spark.shuffle.stream.StreamShuffleHandle;
import org.apache.spark.storage.BlockManager;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Private
public class StreamShuffleWriter<K, V> extends ShuffleWriter<K, V> {
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  private static final int DEFAULT_INIT_SER_BUFFER_SIZE = 1024*1024;
  private final BufExposedByteArrayOutputStream serBuffer;
  private final SerializationStream serOutputStream;
  private final Partitioner partitioner;
  private final StreamedShuffleWriterBackend shuffleWriterBackend;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final StreamShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;

  private static final class BufExposedByteArrayOutputStream extends ByteArrayOutputStream {
    BufExposedByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private final int shuffleId;
  private final SerializerInstance serializerInstance;

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    while (records.hasNext()) {
      Product2<K, V> record = records.next();
      final K key = record._1();
      final int pid = partitioner.getPartition(key);
      serBuffer.reset();
      serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
      serOutputStream.flush();
      int size = serBuffer.size();
      byte[] buf = serBuffer.getBuf();
      shuffleWriterBackend.insertRecord(buf, size, pid);
    }
    shuffleWriterBackend.flush();
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    MapStatus mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), null);
    return Option.apply(mapStatus);
  }

  public StreamShuffleWriter(
          BlockManager blockManager,
          StreamShuffleBlockResolver shuffleBlockResolver,
          TaskMemoryManager memoryManager,
          StreamShuffleHandle<K, V> handle,
          int mapId,
          TaskContext taskContext,
          SparkConf sparkConf) throws IOException {
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.numPartitions = dep.partitioner().numPartitions();
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.serializerInstance = dep.serializer().newInstance();
    this.serBuffer = new BufExposedByteArrayOutputStream(DEFAULT_INIT_SER_BUFFER_SIZE);
    this.serOutputStream = serializerInstance.serializeStream(serBuffer);
    this.shuffleWriterBackend = new StreamedShuffleWriterBackend(numPartitions,
            handle.getBufferedOutputStreams(),
            1024*1024,
            1024*1024);
  }
}
