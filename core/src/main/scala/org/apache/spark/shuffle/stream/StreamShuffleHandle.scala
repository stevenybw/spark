package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, FileOutputStream}

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
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) with Logging {

  val outputStreams = open()

  private def open(): Array[BufferedOutputStream] = {
    val outputStreams = new Array[BufferedOutputStream](numPartitions)
    logInfo(s"Shuffle ${shuffleId} outputs to reducer input files as ${shuffleBlockResolver.getDataFile(shuffleId, 0).getAbsolutePath}, consumes ${1e-6 * numPartitions * fileBufferBytes} MB memory for file buffer")
    for (i <- 0 until numPartitions) {
      val reducerFile = shuffleBlockResolver.getDataFile(shuffleId, i)
      val fos = new FileOutputStream(reducerFile)
      val bos = new BufferedOutputStream(fos, fileBufferBytes)
      outputStreams(i) = bos
    }
    outputStreams
  }

  def getBufferedOutputStreams(): Array[BufferedOutputStream] = {
    outputStreams
  }
}

