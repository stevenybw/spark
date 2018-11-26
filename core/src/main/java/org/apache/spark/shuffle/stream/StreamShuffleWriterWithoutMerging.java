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
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.*;

/**
 * This shuffle method is similar to BypassMergeSortShuffleWriter. One difference is that it does not try to merge
 * the partitions into a single map output file. This is a proof-of-concept that it is not necessary to force a single
 * map output.
 */
public class StreamShuffleWriterWithoutMerging<K, V> extends ShuffleWriter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(StreamShuffleWriterWithoutMerging.class);

  private static final ClassTag<Object> keyClassTag = ClassTag$.MODULE$.Object();
  private static final ClassTag<Object> valueClassTag = ClassTag$.MODULE$.Object();
  private int fileBufferBytes;
  private boolean stopped = false;
  private BlockManager blockManager;
  private SerializerManager serializerManager;
  private SerializerInstance serializerInstance;
  private Partitioner partitioner;
  private int numPartitions;
  private FileOutputStream[] fileOutputStreams;
  private BufferedOutputStream[] bufferedOutputStreams;
  private OutputStream[] compressedOutputStreams;
  private SerializationStream[] serializationStreams;
  private MapStatus mapStatus = null;

  StreamShuffleWriterWithoutMerging(
          BlockManager blockManager,
          StreamShuffleBlockResolver streamShuffleBlockResolver,
          StreamShuffleWithoutMergingHandle<K, V> handle,
          int mapId,
          TaskContext taskContext,
          SparkConf conf) throws IOException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    ShuffleDependency<K, V, V> dep = handle.dependency();
    this.fileBufferBytes = conf.getInt("spark.shuffle.stream.file_buffer_size", 1024*1024);
    this.blockManager = blockManager;
    this.serializerManager = blockManager.serializerManager();
    this.serializerInstance = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.fileOutputStreams = new FileOutputStream[numPartitions];
    this.bufferedOutputStreams = new BufferedOutputStream[numPartitions];
    this.serializationStreams = new SerializationStream[numPartitions];
    for(int i=0; i<numPartitions; i++) {
      ShuffleBlockId blockId = streamShuffleBlockResolver.getDataBlock(dep.shuffleId(), mapId, i);
      File file = streamShuffleBlockResolver.getDataFile(dep.shuffleId(), mapId, i);
      FileOutputStream fos = new FileOutputStream(file);
      BufferedOutputStream bos = new BufferedOutputStream(fos, fileBufferBytes);
      OutputStream cos = serializerManager.wrapStream(blockId, bos);
      SerializationStream ss = serializerInstance.serializeStream(cos);
      fileOutputStreams[i] = fos;
      bufferedOutputStreams[i] = bos;
      compressedOutputStreams[i] = cos;
      serializationStreams[i] = ss;
    }
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    long[] partitionLengths = new long[numPartitions];
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final int pid = partitioner.getPartition(record._1());
      serializationStreams[pid].writeKey(record._1(), keyClassTag);
      serializationStreams[pid].writeValue(record._2(), valueClassTag);
    }
    for(int i=0; i<numPartitions; i++) {
      serializationStreams[i].flush();
      compressedOutputStreams[i].flush();
      bufferedOutputStreams[i].flush();
      fileOutputStreams[i].flush();
      partitionLengths[i] = fileOutputStreams[i].getChannel().size();
      serializationStreams[i].close();
      compressedOutputStreams[i].close();
      bufferedOutputStreams[i].close();
      fileOutputStreams[i].close();
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (!stopped) {
      stopped = true;
      if (success) {
        // return the map status
        return Option.apply(mapStatus);
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
