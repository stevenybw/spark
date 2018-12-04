/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.{SparkConf}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleFlusher
import org.apache.spark.storage.BlockManager

class StreamShuffleFlusher(blockManager: BlockManager, streamShuffleHandle: StreamShuffleHandle[_, _], conf: SparkConf) extends ShuffleFlusher {
  override def flush(): MapStatus = {
    streamShuffleHandle.flush()
    val partitionLengths = streamShuffleHandle.getPartitionLengths()
    MapStatus(blockManager.shuffleServerId, partitionLengths)
  }
}
