/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream

import java.io.BufferedOutputStream

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
  * In streaming shuffle, the incoming records are serialized as soon as they are passed to the shuffle
  * writer and buffered in a serialized form during sorting. This is similar to UnsafeShuffleWriter.
  * However, rather than being buffered, spilled and merged into a map output file for each task, this design
  * merges the outputs from several tasks into a group of per-reducer files. Comparing to SortShuffle,
  * this design has several advantages:
  *
  *   - Bounded memory management: saves more memory for RDD cache
  *
  *   - Spill-free in-memory merging: good efficiency without the overhead of merging the spills
  *
  *   - High I/O efficiency: solves the problem coming from large amount of reducers
  *
  * @param conf
  */
private[spark] class StreamShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  /**
    * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    */
  override val shuffleBlockResolver = new StreamShuffleBlockResolver(conf)

  /**
    * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    */
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Streaming shuffle must support serialized shuffle
    assert(SortShuffleManager.canUseSerializedShuffle(dependency))
    new StreamShuffleHandle[K, V](
      shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
                                handle: ShuffleHandle,
                                mapId: Int,
                                context: TaskContext): ShuffleWriter[K, V] = {
    val env = SparkEnv.get
    new StreamShuffleWriter(
      env.blockManager,
      shuffleBlockResolver.asInstanceOf[StreamShuffleBlockResolver],
      context.taskMemoryManager(),
      handle.asInstanceOf[StreamShuffleHandle[K, V]],
      mapId,
      context,
      env.conf)
  }

  /**
    * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
    * Called on executors by reduce tasks.
    */
  override def getReader[K, C](
                                handle: ShuffleHandle,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    *
    * @return true if the metadata removed successfully, otherwise false.
    */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}

/**
  * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
  * stream shuffle.
  */
class StreamShuffleHandle[K, V](
                                 shuffleId: Int,
                                 numMaps: Int,
                                 dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
  def getBufferedOutputStreams(): Array[BufferedOutputStream] = {
  }
}
