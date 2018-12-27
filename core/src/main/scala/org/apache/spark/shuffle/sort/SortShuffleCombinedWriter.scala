/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.gpft.PerThreadCollectionPool
import org.apache.spark.shuffle.sort.collection.SharedExternalSorter
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/**
  * Based on SharedExternalSorter, which is adapted from ExternalSorter with the following changes:
  *   1.
  *
  */
private[spark] class SortShuffleCombinedWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: SortShuffleCombinedHandle[K, V, C],
    mapId: Int,
    context: TaskContext,
    perThreadCollectionPool: PerThreadCollectionPool[SharedExternalSorter[K, V, C]])
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val sorter = perThreadCollectionPool.borrow(context.taskAttemptId())
    sorter.resetOwnership(mapId, context, dep.aggregator, shuffleBlockResolver)
    mapStatus = dep.partitioner match {
      case _: RandomPartitioner => sorter.insertAllFixedPartitionId(records)
      case _ => sorter.insertAll(records)
    }
    perThreadCollectionPool.give(context.taskAttemptId(), sorter)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      return Option(mapStatus)
    } else {
      return None
    }
  }
}


