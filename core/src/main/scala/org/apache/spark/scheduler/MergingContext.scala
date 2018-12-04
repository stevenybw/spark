/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.shuffle.ShuffleFlusher

/**
  * This is used to generate ShuffleFlushTask
  *
  * @param flusher
  * @param id
  * @param i
  * @param taskBinary
  * @param properties
  * @param serializedTaskMetrics
  * @param maybeInt
  * @param maybeString
  * @param applicationAttemptId
  */
class MergingContext(flusher: ShuffleFlusher,
                     stageId: Int,
                     stageAttemptId: Int,
                     shuffleId: Int,
                     taskBinary: Broadcast[Array[Byte]],
                     localProperties: Properties,
                     serializedTaskMetrics: Array[Byte],
                     jobId: Option[Int],
                     appId: Option[String],
                     appAttemptId: Option[String]) {
  def createFlushTask(host: String, executorId: String, taskId: Int) = {
    val loc = TaskLocation(host, executorId)
    new ShuffleFlushTask(stageId, stageAttemptId, taskBinary, taskId, Array(loc), localProperties, serializedTaskMetrics, jobId, appId, appAttemptId)
  }

  def getShuffleId = shuffleId
}
