/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.sort

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.gpft.PerThreadCollectionPool
import org.apache.spark.shuffle.sort.collection.SharedExternalSorter
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleFlusher}

class SortShuffleCombinedFlusher[K, V, C](
                                           shuffleBlockResolver: IndexShuffleBlockResolver,
                                           handle: SortShuffleCombinedHandle[K, V, C],
                                           flusherId: Int,
                                           context: TaskContext,
                                           perThreadCollectionPool: PerThreadCollectionPool[SharedExternalSorter[K, V, C]])
  extends ShuffleFlusher with Logging {

  private val blockManager = SparkEnv.get.blockManager

  override def flush(): MapStatus = {
    val (sorterOpt, numCollections) = perThreadCollectionPool.popAndGetSize()
    if (sorterOpt.isDefined) {
      val sorter = sorterOpt.get
      sorter.resetOwnership(flusherId, context, handle.dependency.aggregator, shuffleBlockResolver)
      print(s"Flush task with id ${flusherId} flush a sorter (${numCollections} remaining)")
      val mapStatus = sorter.flush()
      sorter.close()
      mapStatus
    } else {
      MapStatus(blockManager.shuffleServerId)
    }
  }
}
