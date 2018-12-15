/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleFlusher
import org.apache.spark.storage.BlockManager

class StreamShuffleFlusherCombined(blockManager: BlockManager, concurrentCombiner: ConcurrentCombiner[_, _, _], conf: SparkConf) extends ShuffleFlusher {
  override def flush(): MapStatus = {
    concurrentCombiner.flush()
    val partitionLengths = concurrentCombiner.filesPartitionedWriter.getPartitionLengths()
    concurrentCombiner.close()
    MapStatus(blockManager.shuffleServerId, partitionLengths)
  }
}
