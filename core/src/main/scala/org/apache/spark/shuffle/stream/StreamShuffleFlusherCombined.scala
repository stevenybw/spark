/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleFlusher}
import org.apache.spark.storage.BlockManager

class StreamShuffleFlusherCombined(blockManager: BlockManager,
                                   handle: BaseShuffleHandle[_, _, _],
                                   concurrentCombiner: ConcurrentCombiner[_, _, _],
                                   taskId: Int,
                                   flusherId: Int,
                                   taskContext: TaskContext,
                                   conf: SparkConf)
  extends ShuffleFlusher with Logging {

  override def flush(): MapStatus = {
    var taskDuration = -System.nanoTime()
    val concurrentCombinerMetrics = new ConcurrentCombinerMetrics
    concurrentCombiner.flush(concurrentCombinerMetrics, flusherId)
    val partitionLengths = concurrentCombiner.filesPartitionedWriter.getPartitionLengths(flusherId)
    concurrentCombiner.close(concurrentCombinerMetrics, flusherId)
    taskDuration += System.nanoTime()
    logInfo("YPerformanceMetric  flush," + taskId +
      "," + handle.dependency.shuffleId +
      "," + concurrentCombinerMetrics.recordsWritten +
      "," + concurrentCombinerMetrics.bytesWritten +
      "," + taskDuration +
      "," + concurrentCombinerMetrics.writeDuration +
      "," + concurrentCombinerMetrics.serializationDuration +
      "," + 0 +
      "," + handle.dependency.mapSideCombine)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    shuffleWriteMetrics.incBytesWritten(concurrentCombinerMetrics.bytesWritten)
    shuffleWriteMetrics.incRecordsWritten(concurrentCombinerMetrics.recordsWritten)
    shuffleWriteMetrics.incWriteTime(concurrentCombinerMetrics.writeDuration)
    MapStatus(blockManager.shuffleServerId, partitionLengths)
  }
}
