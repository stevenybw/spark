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
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class StreamShuffleWriterWithoutMerging<K, V> extends ShuffleWriter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(StreamShuffleWriterWithoutMerging.class);

  private static final ClassTag<Object> keyClassTag = ClassTag$.MODULE$.Object();
  private static final ClassTag<Object> valueClassTag = ClassTag$.MODULE$.Object();
  private boolean stopped = false;
  private BlockManager blockManager;
  private SerializerInstance serializerInstance;
  private Partitioner partitioner;
  private FileOutputStream[] fileOutputStreams;
  private BufferedOutputStream[] bufferedOutputStreams;
  private SerializationStream[] serializationStreams;
  private MapStatus mapStatus = null;
  private int numPartitions;

  StreamShuffleWriterWithoutMerging(
          BlockManager blockManager,
          StreamShuffleBlockResolver shuffleBlockResolver,
          StreamShuffleWithoutMergingHandle<K, V> handle,
          int mapId,
          TaskContext taskContext,
          SparkConf conf) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    ShuffleDependency<K, V, V> dep = handle.dependency();
    this.blockManager = blockManager;
    this.serializerInstance = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    for(int i=0; i<numPartitions; i++) {
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
      bufferedOutputStreams[i].flush();
      partitionLengths[i] = fileOutputStreams[i].getChannel().size();
      serializationStreams[i].close();
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
