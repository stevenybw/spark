/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.BaseShuffleHandle

/**
  * Subclass of [[org.apache.spark.shuffle.BaseShuffleHandle]], used to identify when we've chosen to use the
  * stream shuffle.
  */
private[spark] class StreamShuffleHandle[K, V](
                                 shuffleId: Int,
                                 numMaps: Int,
                                 dependency: ShuffleDependency[K, V, V],
                                 numPartitions: Int,
                                 shuffleBlockResolver: StreamShuffleBlockResolver,
                                 fileBufferBytes: Int)
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) with Logging with BufferedConsumer {

  private var fileChannels: Array[FileChannel] = null
  private var outputStreams: Array[BufferedOutputStream] = null

  private var opened = false
  private var closed = false

  private def open() = {
    if (!opened) {
      opened = true
      logInfo(s"Shuffle ${shuffleId} outputs to reducer input files as ${shuffleBlockResolver.getDataFile(shuffleId, 0).getAbsolutePath}, consumes ${1e-6 * numPartitions * fileBufferBytes} MB memory for file buffer")
      fileChannels = new Array[FileChannel](numPartitions)
      outputStreams = new Array[BufferedOutputStream](numPartitions)
      for (i <- 0 until numPartitions) {
        val reducerFile = shuffleBlockResolver.getDataFile(shuffleId, i)
        val fos = new FileOutputStream(reducerFile)
        fileChannels(i) = fos.getChannel
        val bos = new BufferedOutputStream(fos, fileBufferBytes)
        outputStreams(i) = bos
      }
    }
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
    if (outputStreams == null) {
      open()
    }
    outputStreams
  }

  /**
    * Close the consumer and release the resources
    */
  override def close(): Unit = {
    if (!closed) {
      if (outputStreams != null) {
        for (bos <- outputStreams) {
          bos.close()
        }
      }
      closed = true
    }
  }

  /**
    * Flush the buffered content into downstream
    */
  override def flush(): Unit = {
    if(!closed) {
      if (outputStreams != null) {
        for (bos <- outputStreams) {
          bos.flush()
        }
      }
    }
  }

  /** Get the length (number of bytes) for each reducer partition */
  def getPartitionLengths(): Array[Long] = {
    fileChannels.map(_.size())
  }
}

