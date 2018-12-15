/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, ByteArrayOutputStream, FileOutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
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
                                       aggregator: Aggregator[K, V, C],
                                       serializer: Serializer,
                                       filesPartitionedWriter: FilesPartitionedWriter)
extends Logging {
  private var closed = false
  private val mergeValue: (C, V) => C = aggregator.mergeValue
  private val createCombiner: V => C = aggregator.createCombiner
  // private var kv: Product2[K, V] = null
  // private val update = (hadValue: Boolean, oldValue: C) => {
  //   if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
  // }
  private val serializerInstance = serializer.newInstance()
  private val serBuffer = new BufExposedByteArrayOutputStream(4096)
  private val serStream = serializerInstance.serializeStream(serBuffer)

  @volatile private var map = new PartitionedAppendOnlyMap[K, C]

  private class BufExposedByteArrayOutputStream(size: Int) extends ByteArrayOutputStream(size) {
    def getBuf(): Array[Byte] = buf
  }

  private def writeElement(partitionId: Int, key: K, combinedValue: C): Unit = {
    serBuffer.reset()
    serStream.writeKey(key.asInstanceOf[AnyRef])
    serStream.writeValue(combinedValue.asInstanceOf[AnyRef])
    serStream.flush()
    val buf = serBuffer.getBuf()
    val size = serBuffer.size()
    filesPartitionedWriter.append(partitionId, buf, size)
  }

  /** Spill the in-memory map into PartitionedWriter */
  private def spill(): Unit = {
    logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId} start spilling")
    val inMemoryIterator = map.iterator
    while (inMemoryIterator.hasNext) {
      val record = inMemoryIterator.next()
      val partitionId = record._1._1
      val key = record._1._2
      val combinedValue = record._2
      writeElement(partitionId, key, combinedValue)
    }
    logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId} successfully spilled")
  }

  /**
    * Write a record into this collection. This method is thread-safe.
    * @param record
    */
  def insert(partitionId: Int, record: Product2[K, V]): Unit = synchronized {
    map.changeValue((partitionId, record._1), (hadValue, oldValue) => {
      if (hadValue) mergeValue(oldValue, record._2) else createCombiner(record._2)
    })
    val estimateSize = map.estimateSize()
    if (estimateSize > capacityBytes) {
      spill()
      map = new PartitionedAppendOnlyMap[K, C]
    }
  }

  /**
    * Flush the partitioned append-only map into underlying partitioned writer. But this method will not
    * flush the underlying partitioned writer.
    */
  def flush(): Unit = {
    if (!closed) {
      logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId}'s flush is called, start spilling")
      spill()
      map = new PartitionedAppendOnlyMap[K, C]
    }
  }

  /**
    * Flush the partitioned append-only map into underlying partitioned writer, and release the resources of this
    * shard.
    */
  def close(): Unit = {
    if (!closed) {
      logInfo(s"stage of shuffle id ${shuffleId}  shard id ${shardId}'s close is called, resources released")
      spill()
      map = null
      closed = true
    }
  }
}
