/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.BlockManager

/**
  * The writer for combined stream shuffle
  */
private class StreamShuffleWriterCombined[K, V, C](blockManager: BlockManager,
                                                   handle: StreamShuffleCombinedHandle[K, V, C],
                                                   mapId: Int,
                                                   concurrentCombiner: ConcurrentCombiner[K, V, C])
extends ShuffleWriter[K, V] with Logging {
  var stopped = false

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    require(handle.mapSideCombine, "Map-side combine must be enabled to use combined stream shuffle")
    var numRecords = 0L
    while (records.hasNext) {
      val record = records.next()
      concurrentCombiner.insert(record)
      numRecords += 1
    }
    logInfo(s"Map ${mapId}  NumRecords ${numRecords}")
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (!stopped) {
      stopped = true
      if (success) {
        Option(MapStatus(blockManager.shuffleServerId))
      } else {
        logWarning("Stop unsuccessful shuffle writer")
        None
      }
    } else {
      logWarning("Duplicated stop")
      None
    }
  }
}
