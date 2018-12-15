/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, ShuffleDataBlockId}


/**
  * Comapring to [[org.apache.spark.shuffle.IndexShuffleBlockResolver]], StreamShuffleBlockResolver employs a different
  * format. Each file corresponds to exactly one reducer partition. If the map id is greater than the number of maps of
  * this stage, it will be map to the same name.
  *
  * @param conf
  * @param _blockManager
  */
private[spark] class StreamShuffleBlockResolver(
   conf: SparkConf,
   numMapsForShuffle: ConcurrentHashMap[Int, Int],
   _blockManager: BlockManager = null)
extends ShuffleBlockResolver
with Logging {
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  def getMergedDataBlock(shuffleId: Int, reducerId: Int, numMaps: Int) = {
    ShuffleBlockId(shuffleId, numMaps, reducerId)
  }

  /** The merged data file is a special map output file that belong to the flush task */
  def getMergedDataFile(shuffleId: Int, reducerId: Int, numMaps: Int) = {
    blockManager.diskBlockManager.getFile(getMergedDataBlock(shuffleId, reducerId, numMaps))
  }

  def getDataBlock(shuffleId: Int, mapperId: Int, reducerId: Int) = {
    ShuffleBlockId(shuffleId, mapperId, reducerId)
  }

  def getDataFile(shuffleId: Int, mapperId: Int, reducerId: Int) = {
    blockManager.diskBlockManager.getFile(getDataBlock(shuffleId, mapperId, reducerId))
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val shuffleId = blockId.shuffleId
    val numMaps = numMapsForShuffle.getOrDefault(shuffleId, -1)
    val mapId = blockId.mapId
    val reducerId = blockId.reduceId
    val dataFile = if (mapId < numMaps) getDataFile(shuffleId, mapId, reducerId) else getMergedDataFile(shuffleId, reducerId, numMaps)
    // logInfo(s"getBlockData (shuffleId = ${shuffleId}  mapId = ${mapId}  reducerId = ${reducerId}  fileSize = ${dataFile.length()}")
    new FileSegmentManagedBuffer(
      transportConf,
      dataFile,
      0,
      dataFile.length()
    )
  }

  override def stop(): Unit = {}
}

private[spark] object StreamShuffleBlockResolver {
  // No-op map ID used in interactions with disk store.
  // The outputs for each reducer are glommed into a single file
  val NOOP_MAP_ID = 0
}