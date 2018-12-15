/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.shuffle.ShuffleFlusher

/**
  * This is created in DAGScheduler and stores the information required to create ShuffleFlushTask
  */
class FlushTaskCreater(stageId: Int,
                       stageAttemptId: Int,
                       shuffleId: Int,
                       taskBinary: Broadcast[Array[Byte]],
                       localProperties: Properties,
                       serializedTaskMetrics: Array[Byte],
                       jobId: Option[Int],
                       appId: Option[String],
                       appAttemptId: Option[String]) {
  def createFlushTask(host: String, executorId: String, taskId: Int): ShuffleFlushTask = {
    val loc = TaskLocation(host, executorId)
    new ShuffleFlushTask(stageId, stageAttemptId, taskBinary, taskId, Array(loc), localProperties, serializedTaskMetrics, executorId, jobId, appId, appAttemptId)
  }

  def getShuffleId: Int = shuffleId
}
