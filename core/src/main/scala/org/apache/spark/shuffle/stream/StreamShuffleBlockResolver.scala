/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream

import java.io.File

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, ShuffleDataBlockId}


/**
  * Comapring to [[org.apache.spark.shuffle.IndexShuffleBlockResolver]], StreamShuffleBlockResolver will not merge
  * the output partitions into a single map output file. The message from each mapper to each reducer is a single
  * file.
  *
  * This design seems to produce too many files (M*R files). But this enables Column Shuffle Format and we can
  * significantly reduce the number of files to E*R (E: Number of Executors) by executor-side merging.
  *
  * @param conf
  * @param _blockManager
  */
private[spark] class StreamShuffleBlockResolver(
   conf: SparkConf,
   _blockManager: BlockManager = null)
extends ShuffleBlockResolver
with Logging {
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  def getDataBlock(shuffleId: Int, reducerId: Int) = {
    ShuffleBlockId(shuffleId, StreamShuffleBlockResolver.NOOP_MAP_ID, reducerId)
  }

  def getDataFile(shuffleId: Int, reducerId: Int) = {
    blockManager.diskBlockManager.getFile(getDataBlock(shuffleId, reducerId))
  }

  def getDataBlock(shuffleId: Int, mapperId: Int, reducerId: Int) = {
    ShuffleBlockId(shuffleId, mapperId, reducerId)
  }

  def getDataFile(shuffleId: Int, mapperId: Int, reducerId: Int) = {
    blockManager.diskBlockManager.getFile(getDataBlock(shuffleId, mapperId, reducerId))
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val shuffleId = blockId.shuffleId
    val mapId = blockId.mapId
    val reducerId = blockId.reduceId
    val dataFile = getDataFile(shuffleId, mapId, reducerId)
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