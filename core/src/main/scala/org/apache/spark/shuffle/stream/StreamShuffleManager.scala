/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
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
  *   - Spill-free in-memory merging: good efficiency without the overhead of merging the spills
  *   - High I/O efficiency: solves the problem coming from large amount of reducers
  *
  * There are several challenge to implement such technique:
  *
  *   - Shuffle file format (CSF). Spark's default shuffler SortShuffler assumes that each shuffle task generates
  *   exactly one index file and one data file, which is implemented in IndexShuffleBlockResolver. However,
  *   streaming shuffler requires that a single shuffle task is allowed to generate multiple files (one file
  *   per reducer partition). We provide [[StreamShuffleBlockResolver]] to adapt to this format.
  *
  * We have implemented several versions:
  *   - As a proof-of-concept to demonstrates that we can implement CSF in Spark, we implement
  *   [[StreamShuffleWriterWithoutMerging]]. This is the same as BypassMergeSortShuffle, except that we
  *   do not try to merge the output files into one map output file. The problems of this design:
  *     * Inefficiency when R is large. [[SortShuffleManager]] tackles this problem by requiring that R<=200 to apply
  *     bypass-merge-sort shuffle. When R>200, SortShuffle uses UnsafeShuffleWriter.
  *     * There would be O(M*R) small files, which would lead to pressures to file system. Our solution is
  *     to apply executor-side merging.
  *
  *   - As a proof-of-concept to demonstrates that by executor-side merging we can reduce the number of files to
  *   O(R), we adapt [[StreamShuffleWriterWithoutMerging]] to [[StreamShuffleWriterDirect]]. For each record, it will
  *   append the result directly into shared buffer.
  *
  *   - Enabling executor-side merging, we implement [[StreamShuffleWriter]]. This version ignores fault tolerant and
  *   is to demonstrate the performance aspect. A shared buffered output streams is stored in StreamShuffleHandle. The
  *   StreamShuffleWriter do the serialization and sorting locally, and append the content into the shared output streams.
  *   One key problem is: when the executor drain the buffer and add the mapOutput to the driver? Obviously, when the
  *   shuffle stage has been finished. So,
  *   This version is a demo for performance, and totally ignore the fault tolerance.
  *
  * @param conf
  */
private[spark] class StreamShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  private val fileBufferBytes = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
  private val shuffleMethod = conf.get("spark.shuffle.stream.method", "unmerged_direct")

  // Very strange that this should be set in getWriter, not in registerShuffle, strange. Who is calling registerShuffle?
  private[stream] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  /**
    * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    */
  override val shuffleBlockResolver = new StreamShuffleBlockResolver(conf, numMapsForShuffle)

  /**
    * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    */
  override def registerShuffle[K, V, C](shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val numPartitions = dependency.partitioner.numPartitions
    shuffleMethod match {
      case "merged_batched" =>
        logInfo(s"StreamShuffle registered: id = ${shuffleId}  numMaps = ${numMaps}   numReducers = ${numPartitions}")
        // Streaming shuffle must support serialized shuffle
        assert(SortShuffleManager.canUseSerializedShuffle(dependency))
        new StreamShuffleHandle[K, V](
          shuffleId,
          numMaps,
          dependency.asInstanceOf[ShuffleDependency[K, V, V]],
          numPartitions)
      case "unmerged_direct" =>
        logInfo(s"StreamShuffleWithoutMerging registered: id = ${shuffleId}  numMaps = ${numMaps}   numReducers = ${numPartitions}")
        assert(!dependency.mapSideCombine, "Unable to use stream shuffle manager with mapSideCombine enabled");
        new StreamShuffleWithoutMergingHandle[K, V](
          shuffleId,
          numMaps,
          dependency.asInstanceOf[ShuffleDependency[K, V, V]])
      case "merged_direct" =>
        logInfo(s"StreamShuffleDirect registered: id = ${shuffleId}  numMaps = ${numMaps}   numReducers = ${numPartitions}")
        assert(dependency.serializer.supportsRelocationOfSerializedObjects)
        assert(!dependency.mapSideCombine, "Unable to use serialized shuffle with mapSideCombine enabled")
        new StreamShuffleDirectHandle[K, V](
          shuffleId,
          numMaps,
          dependency.asInstanceOf[ShuffleDependency[K, V, V]],
          numPartitions
        )
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
                                handle: ShuffleHandle,
                                mapId: Int,
                                context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_,_,_]].numMaps)
    val env = SparkEnv.get
    handle match {
      case streamShuffleHandle: StreamShuffleHandle[K @unchecked, V @unchecked] =>
        val shuffleId = streamShuffleHandle.shuffleId
        val numMaps = streamShuffleHandle.numMaps
        val numPartitions = streamShuffleHandle.numPartitions
        val sharedObjectManager = env.sharedObjectManager
        val objid = StreamShuffleManager.STREAM_SHUFFLE_PREFIX + shuffleId.toString
        val sharedWriter = sharedObjectManager.getOrCreate(objid, (x: String) => new SharedWriter(
            shuffleId,
            numMaps,
            numPartitions,
            shuffleBlockResolver,
            fileBufferBytes))
        logInfo(s"Task ${mapId} from batched shuffle ${shuffleId} get the shared writer from SharedObjectManager: ${objid}")
        new StreamShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[StreamShuffleBlockResolver],
          context.taskMemoryManager(),
          streamShuffleHandle,
          mapId,
          context,
          env.conf,
          sharedWriter)
      case streamShuffleWithoutMergingHandle: StreamShuffleWithoutMergingHandle[K @unchecked, V @unchecked] =>
        new StreamShuffleWriterWithoutMerging(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[StreamShuffleBlockResolver],
          streamShuffleWithoutMergingHandle,
          mapId,
          context,
          env.conf)
      case streamShuffleDirectHandle: StreamShuffleDirectHandle[K @unchecked, V @unchecked] =>
        val shuffleId = streamShuffleDirectHandle.shuffleId
        val numMaps = streamShuffleDirectHandle.numMaps
        val numPartitions = streamShuffleDirectHandle.numPartitions
        val sharedObjectManager = env.sharedObjectManager
        val objid = StreamShuffleManager.STREAM_SHUFFLE_PREFIX + shuffleId.toString
        val sharedWriter = sharedObjectManager.getOrCreate(objid, (x: String) => new SharedWriter(
          shuffleId,
          numMaps,
          numPartitions,
          shuffleBlockResolver,
          fileBufferBytes))
        logInfo(s"Task ${mapId} from direct shuffle ${shuffleId} get the shared writer from SharedObjectManager: ${objid}")
        new StreamShuffleWriterDirect(
          env.blockManager,
          shuffleBlockResolver,
          streamShuffleDirectHandle,
          mapId,
          context,
          sharedWriter,
          env.conf)
    }
  }


  override def getFlusher(handle: ShuffleHandle): Option[ShuffleFlusher] = {
    val env = SparkEnv.get
    handle match {
      case streamShuffleHandle: StreamShuffleHandle[_, _] =>
        val shuffleId = streamShuffleHandle.shuffleId
        val sharedObjectManager = env.sharedObjectManager
        val objid = StreamShuffleManager.STREAM_SHUFFLE_PREFIX + shuffleId.toString
        val sharedWriter = sharedObjectManager.get(objid).asInstanceOf[SharedWriter]
        logInfo(s"Shuffle flush task from shuffle ${shuffleId} get the shared writer from SharedObjectManager: ${objid}")
        Option(new StreamShuffleFlusher(
          env.blockManager,
          sharedWriter,
          env.conf))
      case streamShuffleDirectHandle: StreamShuffleDirectHandle[_, _] =>
        val shuffleId = streamShuffleDirectHandle.shuffleId
        val sharedObjectManager = env.sharedObjectManager
        val objid = StreamShuffleManager.STREAM_SHUFFLE_PREFIX + shuffleId.toString
        val sharedWriter = sharedObjectManager.get(objid).asInstanceOf[SharedWriter]
        logInfo(s"Shuffle flush task from shuffle ${shuffleId} get the shared writer from SharedObjectManager: ${objid}")
        Option(new StreamShuffleFlusher(
          env.blockManager,
          sharedWriter,
          env.conf))

      case streamShuffleWithoutMergingHandle: StreamShuffleWithoutMergingHandle[_, _] =>
        None
    }
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

private[spark] class StreamShuffleHandle[K, V](shuffleId: Int,
                                               numMaps: Int,
                                               dependency: ShuffleDependency[K, V, V],
                                               val numPartitions: Int)
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

private[spark] class StreamShuffleDirectHandle[K, V](shuffleId: Int,
                                                     numMaps: Int,
                                                     dependency: ShuffleDependency[K, V, V],
                                                     val numPartitions: Int)
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

private[spark] class StreamShuffleWithoutMergingHandle[K, V](shuffleId: Int,
                                                             numMaps: Int,
                                                             dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) with Logging {
}


object StreamShuffleManager {
  val STREAM_SHUFFLE_PREFIX = "stream_shuffle_"
}
