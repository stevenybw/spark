/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

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
  private val closedShards = new mutable.HashSet[Int]()

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
    if (!closedShards.contains(shardId)) {
      shards(shardId).flush(metrics)
      filesPartitionedWriter.flush(metrics, shardId)
    }
  }

  /**
    * Flush and close
    */
  def close(metrics: ConcurrentCombinerMetrics, shardId: Int): Unit = {
    if (!closedShards.contains(shardId)) {
      shards(shardId).close(metrics)
      shards(shardId) = null
      filesPartitionedWriter.close(shardId)
      closedShards.add(shardId)
      if (closedShards.size == numShards) {
        shards = null
        filesPartitionedWriter = null
        closed = true
      }
    }
  }
}
