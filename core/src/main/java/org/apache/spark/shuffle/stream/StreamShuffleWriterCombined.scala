/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.TaskContext
import org.apache.spark.executor.ShuffleWriteMetrics
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
                                                   taskContext: TaskContext,
                                                   concurrentCombiner: ConcurrentCombiner[K, V, C])
extends ShuffleWriter[K, V] with Logging {
  var stopped = false

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    require(handle.mapSideCombine, "Map-side combine must be enabled to use combined stream shuffle")
    var taskDuration = -System.nanoTime()
    val concurrentCombinerMetrics = new ConcurrentCombinerMetrics
    var numRecords = 0L
    while (records.hasNext) {
      val record = records.next()
      concurrentCombiner.insert(record, concurrentCombinerMetrics)
      numRecords += 1
    }
    taskDuration += System.nanoTime()
    logInfo("YPerformanceMetric  map," + mapId +
      "," + handle.dependency.shuffleId +
      "," + concurrentCombinerMetrics.recordsWritten +
      "," + concurrentCombinerMetrics.bytesWritten +
      "," + taskDuration +
      "," + concurrentCombinerMetrics.writeDuration +
      "," + concurrentCombinerMetrics.serializationDuration +
      "," + numRecords +
      "," + handle.dependency.mapSideCombine)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    shuffleWriteMetrics.incBytesWritten(concurrentCombinerMetrics.bytesWritten)
    shuffleWriteMetrics.incRecordsWritten(concurrentCombinerMetrics.recordsWritten)
    shuffleWriteMetrics.incWriteTime(concurrentCombinerMetrics.writeDuration)
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
