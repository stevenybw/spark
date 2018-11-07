/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

class MetadataShuffleBlockResolver extends ShuffleBlockResolver {

  /**
    * Retrieve the data for the specified block. If the data for that block is not available,
    * throws an unspecified exception.
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = ???

  override def stop(): Unit = ???
}
