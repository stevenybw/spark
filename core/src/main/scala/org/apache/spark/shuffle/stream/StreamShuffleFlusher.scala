/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.{SparkConf}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleFlusher
import org.apache.spark.storage.BlockManager

class StreamShuffleFlusher(blockManager: BlockManager, sharedWriter: SharedWriter, conf: SparkConf) extends ShuffleFlusher {
  override def flush(): MapStatus = {
    sharedWriter.flush()
    val partitionLengths = sharedWriter.getPartitionLengths()
    sharedWriter.close()
    MapStatus(blockManager.shuffleServerId, partitionLengths)
  }
}
