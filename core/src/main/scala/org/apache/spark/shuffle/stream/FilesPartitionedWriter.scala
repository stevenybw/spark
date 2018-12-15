/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * The partitioned writer based on creating numPartitions files. It targets for optimized I/O by combining fragmented
  * writes into large continuous write that is suitable for that I/O device.
  *
  * This container is sharded by round-robin and enables parallel flushing
  */
private[spark] class FilesPartitionedWriter (
  shuffleId: Int,
  numMaps: Int,
  numPartitions: Int,
  numShards: Int,
  shuffleBlockResolver: StreamShuffleBlockResolver,
  fileBufferBytes: Int) extends Logging {

  // In-memory merging state should not be passed to others
  private var fileChannels = new Array[FileChannel](numPartitions)
  private var outputStreams = new Array[BufferedOutputStream](numPartitions)
  private var closedShards = new mutable.HashSet[Int]()
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

  /**
    * We can also wrap the
    */
//  def getPartitionOutputStream(partitionId: Int): PartitionOutputStream = {
//
//  }

  /**
    * Append to partitionId's shuffle output with an exclusive lock
    */
  def append(partitionId: Int, buffer: Array[Byte], bytes: Int): Unit = {
    outputStreams(partitionId).write(buffer, 0, bytes)
  }

  /**
    * Close the consumer and release the resources
    */
  def close(shardId: Int): Unit = {
    if (!closedShards.contains(shardId)) {
      for (i <- 0 until outputStreams.length) {
        if (FilesPartitionedWriter.partitionBelongsToShard(i, numShards, shardId)) {
          outputStreams(i).flush()
          outputStreams(i).close()
          outputStreams(i) = null
        }
      }
      closedShards.add(shardId)
      if (closedShards.size == numShards) {
        fileChannels = null
        outputStreams = null
        closed = true
      }
    }
  }

  /**
    * Flush the buffered content into downstream
    */
  def flush(metrics: ConcurrentCombinerMetrics, shardId: Int): Unit = {
    if(!closedShards.contains(shardId)) {
      metrics.writeDuration -= System.nanoTime()
      for (i <- 0 until outputStreams.length) {
        if (FilesPartitionedWriter.partitionBelongsToShard(i, numShards, shardId)) {
          outputStreams(i).flush()
        }
      }
      metrics.writeDuration += System.nanoTime()
    }
  }

  /** Get the length (number of bytes) for each reducer partition */
  def getPartitionLengths(shardId: Int): Array[Long] = {
    (0 until fileChannels.length).map(partitionId => {
      if (FilesPartitionedWriter.partitionBelongsToShard(partitionId, numShards, shardId)) {
        fileChannels(partitionId).size()
      } else {
        0L
      }
    }).toArray
  }
}

object FilesPartitionedWriter {
  def partitionBelongsToShard(partitionId: Int, numShards: Int, shardId: Int): Boolean = {
    (partitionId % numShards) == shardId
  }
}
