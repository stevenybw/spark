/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet
import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

import scala.collection.mutable

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    mapOutputTrackerMaster: MapOutputTrackerMaster)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {


  private[this] var _isDraining = false
  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause paritition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]

  /** Shuffle flush tasks expected (host, execId) */
  // val pendingShuffleFlushTasks = new HashSet[(String, String)]

  /** Expected ShuffleFlushTasks (hostId, execId) to a set of taskIndex */
  // val pendingFlushTasks = new mutable.HashMap[(String, String), mutable.Set[Int]]()

  /**
    * How many ShuffleFlushTask to launch for each executor.
    *
    * Warning: Be aware to stay synchronized with the number of shards of
    * [[org.apache.spark.shuffle.stream.ConcurrentCombiner]]. This is about correctness, not performance.
    */
  val numFlushTasksPerExecutor: Int = 1<<rdd.conf.getInt("spark.shuffle.stream.combine.numShardsPO2", 4)

  /** Expected ShuffleFlushTasks index */
  val pendingFlushTasks = new mutable.HashSet[Int]()
  val allFlushTasks = new mutable.HashSet[Int]()

  def numFlushTasks: Int = allFlushTasks.size

  /** Get task index from executorId and flusherId pair */
  def taskIndexFromExecutorIdAndFlusherId(executorId: String, flusherId: Int): Int = {
    org.apache.spark.executor.Executor.convertExecutorId(executorId) * numFlushTasksPerExecutor + flusherId
  }

  /** Add a new executor for flush tasks */
  def addExecutorForFlushing(host: String, executorId: String): Unit = {
    for (i <- 0 until numFlushTasksPerExecutor) {
      // Pass global task id ( a task is a flush task if task id >= numTasks )
      val taskId = numTasks + taskIndexFromExecutorIdAndFlusherId(executorId, i)
      pendingFlushTasks.add(taskId)
      allFlushTasks.add(taskId)
    }
  }

  /** Complete a ShuffleMapTask */
  def completeShuffleMapTask(taskIndex: Int): Unit = {
    pendingFlushTasks.remove(taskIndex)
  }

  /** When all the pending partitions have finished, the stage enters into draining state */
  def isDraining: Boolean = _isDraining

  /** Return true if this stage require flushing */
  def flushRequired: Boolean = {
    SparkEnv.get.shuffleManager.flushRequired(shuffleDep.shuffleHandle)
  }

  /**
    * Transform this stage from serving to draining, this will initiate pendingShuffleFlushTasks
    */
  def startDraining(): Unit = {
    require(!_isDraining)
    _isDraining = true
    val shuffleId = shuffleDep.shuffleId
    mapOutputTrackerMaster
      .shuffleStatuses(shuffleId)
      .mapStatuses
      .map(m => (m.location.host, m.location.executorId))
      .distinct
      .foreach(t => addExecutorForFlushing(t._1, t._2))
  }

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}
