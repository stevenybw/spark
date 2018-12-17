/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

import scala.collection.mutable

/**
  * A [[ConcurrentCombiner]] performs partial aggregation without sorting and spills in-memory data to a [[FilesPartitionedWriter]].
  * A [[ConcurrentCombiner]] is composed of multiple [[ConcurrentCombinerShard]] (which is specified by numParallelism). We assign
  * the incoming records to shards in a round-robin fashion. E.g., to insert a record of partition i, we assign the record to i%numParallelism.
  * Modular is very slow. To optimize the performance, we require numParallelism to be a power of two.
  *
  * WARNING: The aggregator is not thread safe because a [[org.apache.spark.serializer.SerializerInstance]] may be cached
  * inside like operations such as foldByKey. We have to accept aggregator from function argument.
  *
  * @param shuffleId the shuffle id of this ShuffleMapStage
  * @param numShardsPowerOfTwo total number of shards in this stage
  * @param totalMemoryCapacity memory capacity this combiner is allowed to use
  * @param partitioner the partitioner
  * @param aggregator the aggregator used for combining
  * @param serializer the serializer used for writing to file
  * @param filesPartitionedWriter [move in] the partitioned writer
  */
class ConcurrentCombiner[K, V, C](shuffleId: Int,
                                  numShardsPowerOfTwo: Int,
                                  totalMemoryCapacity: Long,
                                  partitioner: Partitioner,
                                  aggregator: Aggregator[K, V, C],
                                  serializer: Serializer,
                                  private[stream] var filesPartitionedWriter: FilesPartitionedWriter)
extends Logging {
  private var closed = false
  private val numShards = 1<<numShardsPowerOfTwo

  // private val closedShards = new mutable.HashSet[Int]() // BUG: Should be concurrent
  private val _closedShards = new ConcurrentHashMap[Int, Unit]()
  def isShardClosed(shardId: Int): Boolean = _closedShards.contains(shardId)
  def setShardClosed(shardId: Int): Unit = _closedShards.put(shardId, Unit)
  def numClosedShards(): Int = _closedShards.size()

  private def partitionIdFromLocalId(localId: Int, shardId: Int): Int = (localId<<numShardsPowerOfTwo) + shardId

  private def localIdFromPartitionId(partitionId: Int): Int = partitionId>>numShardsPowerOfTwo

  /** sid = pid % numShards*/
  private def shardIdFromPartitionId(partitionId: Int): Int = partitionId & (numShards-1)

  // private def getNumPartitions(shardId: Int): Int = if (shardId < numPartitions % numShards) numPartitions/numShards+1 else numPartitions/numShards

  private var shards = (0 until numShards).map(shardId => new ConcurrentCombinerShard(shuffleId,
    shardId,
    numShardsPowerOfTwo,
    totalMemoryCapacity / numShards,
    aggregator,
    serializer,
    filesPartitionedWriter
  )).toArray

  /**
    * Write a record into this collection. This method is thread-safe.
    * @param record
    */
  def insert(record: Product2[K, V], metrics: ConcurrentCombinerMetrics, aggregator: Aggregator[K, V, C]): Unit = {
    val partitionId = partitioner.getPartition(record._1)
    val shardId = shardIdFromPartitionId(partitionId)
    shards(shardId).insert(partitionId, record, metrics, aggregator)
  }

  /**
    * Flush the buffered data into underlying partitioned writer, and then flush the partitioned writer into
    * underlying file system.
    */
  def flush(metrics: ConcurrentCombinerMetrics, shardId: Int): Unit = {
    if (!isShardClosed(shardId)) {
      shards(shardId).flush(metrics)
      filesPartitionedWriter.flush(metrics, shardId)
    }
  }

  /**
    * Flush and close
    */
  def close(metrics: ConcurrentCombinerMetrics, shardId: Int): Unit = {
    if (!isShardClosed(shardId)) {
      shards(shardId).close(metrics)
      shards(shardId) = null
      filesPartitionedWriter.close(shardId)
      setShardClosed(shardId)
      if (numClosedShards() == numShards) {
        shards = null
        filesPartitionedWriter = null
        closed = true
      }
    }
  }
}

object ConcurrentCombiner {
  /**
    *
    * @param logInfo
    * @param phase
    * @param taskId
    * @param shuffleId
    * @param recordsProcessed
    * @param recordsWritten
    * @param bytesWritten
    * @param taskTimeNs The time of the whole task
    * @param outerInsertNs The time of insertion (before the lock is acquired)
    * @param innerInsertNs The time of insertion (after the lock is acquired)
    * @param writeTimeNs The time blocked on I/O write
    * @param serializationTimeNs The time for serialization
    * @param mapSideCombine Whether to use mapSideCombine
    */
  def performanceLog(shuffler: String,
                     phase: String,
                     taskId: Int,
                     shuffleId: Int,
                     recordsProcessed: Long,
                     recordsWritten: Long,
                     bytesWritten: Long,
                     taskTimeNs: Long,
                     outerInsertNs: Long,
                     innerInsertNs: Long,
                     writeTimeNs: Long,
                     serializationTimeNs: Long,
                     mapSideCombine: Boolean): String = {
    s"YPerformanceMetric  ${shuffler},${phase},${taskId},${shuffleId},${recordsProcessed},${recordsWritten}," +
      s"${bytesWritten},${taskTimeNs},${outerInsertNs},${innerInsertNs},${writeTimeNs},${serializationTimeNs},${mapSideCombine}"
  }
}
