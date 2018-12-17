/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, ByteArrayOutputStream, FileOutputStream}
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency}
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

/**
  * A shard of ConcurrentCombiner that employs PartitionedAppendOnlyMap as the size-tracking container to perform partial
  * aggregation. Comparing to [[org.apache.spark.util.collection.ExternalSorter]], which sorts and spills in-memory data to disk for
  * further merging, [[ConcurrentCombinerShard]] only performs partial aggregation without sorting and spills in-memory data to a
  * [[FilesPartitionedWriter]].
  *
  * @param shuffleId the shuffle id of this ShuffleMapStage
  * @param shardId the shard id
  * @param numShardsPowerOfTwo total number of shards in this stage
  * @param capacityBytes the capacity of this shard
  * @param aggregator the aggregator used for combining
  * @param serializer the serializer used for writing to file
  * @param filesPartitionedWriter the partitioned writer
  */
class ConcurrentCombinerShard[K, V, C](shuffleId: Int,
                                       shardId: Int,
                                       numShardsPowerOfTwo: Int,
                                       capacityBytes: Long,
                                       _aggregator_do_not_use: Aggregator[K, V, C],
                                       serializer: Serializer,
                                       filesPartitionedWriter: FilesPartitionedWriter)
extends Logging {
  private var closed = false
  private val lock = new ReentrantLock()
  // private val mergeValue: (C, V) => C = aggregator.mergeValue
  // private val createCombiner: V => C = aggregator.createCombiner
  // private var kv: Product2[K, V] = null
  // private val update = (hadValue: Boolean, oldValue: C) => {
  //   if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
  // }

  @volatile private var map = new PartitionedAppendOnlyMap[K, C]

  private class BufExposedByteArrayOutputStream(size: Int) extends ByteArrayOutputStream(size) {
    def getBuf(): Array[Byte] = buf
  }

  private def writeElement(partitionId: Int,
                           key: K,
                           combinedValue: C,
                           metrics: ConcurrentCombinerMetrics,
                           serBuffer: BufExposedByteArrayOutputStream,
                           serStream: SerializationStream): Unit = {
    metrics.serializationDuration -= System.nanoTime()
    serBuffer.reset()
    serStream.writeKey(key.asInstanceOf[AnyRef])
    serStream.writeValue(combinedValue.asInstanceOf[AnyRef])
    serStream.flush()
    metrics.serializationDuration += System.nanoTime()
    val buf = serBuffer.getBuf()
    val size = serBuffer.size()
    metrics.writeDuration -= System.nanoTime()
    filesPartitionedWriter.append(partitionId, buf, size)
    metrics.writeDuration += System.nanoTime()
    metrics.recordsWritten += 1
    metrics.bytesWritten += size
  }

  /** Spill the in-memory original map into PartitionedWriter */
  private def spill(originalMap: PartitionedAppendOnlyMap[K, C], metrics: ConcurrentCombinerMetrics): Unit = {
    logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId} start spilling")
    val serializerInstance = serializer.newInstance()
    val serBuffer = new BufExposedByteArrayOutputStream(4096)
    val serStream = serializerInstance.serializeStream(serBuffer)
    val inMemoryIterator = originalMap.iterator
    while (inMemoryIterator.hasNext) {
      val record = inMemoryIterator.next()
      val partitionId = record._1._1
      val key = record._1._2
      val combinedValue = record._2
      writeElement(partitionId, key, combinedValue, metrics, serBuffer, serStream)
    }
    logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId} successfully spilled")
  }

  /**
    * Write a record into this collection. This method is thread-safe.
    * @param record
    */
  def insert(partitionId: Int, record: Product2[K, V], metrics: ConcurrentCombinerMetrics, aggregator: Aggregator[K, V, C]): Unit = {
    val createCombiner = aggregator.createCombiner
    val mergeValue = aggregator.mergeValue
    val updateFunc = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, record._2) else createCombiner(record._2)
    }
    lock.lock()
    metrics.innerInsertDuration -= System.nanoTime()
    map.changeValue((partitionId, record._1), updateFunc)
    val estimateSize = map.estimateSize()
    if (estimateSize > capacityBytes) {
      val originalMap = map
      map = new PartitionedAppendOnlyMap[K, C]
      lock.unlock()
      spill(originalMap, metrics)
    } else {
      lock.unlock()
    }
    metrics.innerInsertDuration += System.nanoTime()
  }

  /**
    * Flush the partitioned append-only map into underlying partitioned writer. But this method will not
    * flush the underlying partitioned writer.
    */
  def flush(metrics: ConcurrentCombinerMetrics): Unit = {
    if (!closed) {
      lock.lock()
      val originalMap = map
      map = new PartitionedAppendOnlyMap[K, C]
      lock.unlock()
      logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId}'s flush is called, start spilling")
      spill(originalMap, metrics)
    }
  }

  /**
    * Flush the partitioned append-only map into underlying partitioned writer, and release the resources of this
    * shard.
    */
  def close(metrics: ConcurrentCombinerMetrics): Unit = {
    if (!closed) {
      lock.lock()
      val originalMap = map
      map = null
      lock.unlock()
      logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId}'s close is called, resources released")
      spill(originalMap, metrics)
      closed = true
    }
  }
}
