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
  * Create and maintain the shuffle block's mapping between logic block and physical file location.
  * Data of shuffle blocks to the same reduce task are stored in a single consolidated data file.
  * @param conf
  * @param _blockManager
  */
private[spark] class StreamShuffleBlockResolver(
   conf: SparkConf,
   _blockManager: BlockManager = null)
extends ShuffleBlockResolver
with Logging {
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle");

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val shuffleId = blockId.shuffleId
    val mapId = blockId.mapId
    val reduceId = blockId.reduceId
    if (mapId != StreamShuffleBlockResolver.NOOP_MAP_ID) {
      throw new Exception(s"In stream shuffle mode, mapId must be 0 and there is only one mapper");
    }
    val dataFile = blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, StreamShuffleBlockResolver.NOOP_MAP_ID, reduceId))
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