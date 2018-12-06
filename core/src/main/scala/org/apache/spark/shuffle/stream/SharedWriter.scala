/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.internal.Logging

/**
  * Shared writer is a lazy-initialized collection of buffered file output streams.
  */
private[spark] class SharedWriter(
  shuffleId: Int,
  numMaps: Int,
  numPartitions: Int,
  shuffleBlockResolver: StreamShuffleBlockResolver,
  fileBufferBytes: Int) extends Logging with BufferedConsumer {

  // In-memory merging state should not be passed to others
  private var fileChannels = new Array[FileChannel](numPartitions)
  private var outputStreams = new Array[BufferedOutputStream](numPartitions)
  private var closed = false

  logInfo(s"Shuffle ${shuffleId} outputs to reducer input files as ${shuffleBlockResolver.getMergedDataFile(shuffleId, 0, numMaps).getAbsolutePath}, consumes ${1e-6 * numPartitions * fileBufferBytes} MB memory for file buffer")
  for (i <- 0 until numPartitions) {
    val reducerFile = shuffleBlockResolver.getMergedDataFile(shuffleId, i, numMaps)
    val fos = new FileOutputStream(reducerFile)
    fileChannels(i) = fos.getChannel
    val bos = new BufferedOutputStream(fos, fileBufferBytes)
    outputStreams(i) = bos
  }

  /**
    * We only open the files and allocates the memory after the launch of the first task of this shuffle
    *
    * @return
    */
  def getBufferedOutputStreams(): Array[BufferedOutputStream] = {
    if (closed) {
      throw new Exception("Try to get buffered output streams from a closed handle")
    }
    outputStreams
  }

  def append(partitionId: Int, buffer: Array[Byte], bytes: Int): Unit = {
    outputStreams(partitionId).write(buffer, 0, bytes)
  }

  /**
    * Close the consumer and release the resources
    */
  override def close(): Unit = {
    if (!closed) {
      for (bos <- outputStreams) {
        bos.flush()
        bos.close()
      }
      fileChannels = null
      outputStreams = null
      closed = true
    }
  }

  /**
    * Flush the buffered content into downstream
    */
  override def flush(): Unit = {
    if(!closed) {
      for (bos <- outputStreams) {
        bos.flush()
      }
    }
  }

  /** Get the length (number of bytes) for each reducer partition */
  def getPartitionLengths(): Array[Long] = {
    fileChannels.map(_.size())
  }
}
