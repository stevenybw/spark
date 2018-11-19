/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.sort.stream;

import io.netty.buffer.ByteBuf;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Java-based backend for streamed shuffle writing
 *
 * It provides basic functionality for streamed shuffle writing. Each task owns a streamed shuffle writer backend, which
 * is associated with an array of output streams at creation.
 *
 * <b>Implementation</b>
 * How to perform radix sort on variable-length record?
 *   1. (*) Single-pass radix sort: radix-sort the indexes and generate the sorted data according to the index. Random access.
 *   2. Multi-pass radix sort: radix-sort both the indexes and the data. Sequential access.
 */
public class StreamedShuffleWriterBackend {
  // Constants
  private static final int BATCH_BUFFER_MAX_NUM = 1 * 1024*1024;
  private static final int BATCH_BUFFER_CAPACITY = 16 * 1024*1024;

  // Batch states
  private int numPartitions;
  private BufferedOutputStream[] bufferedOutputStreams;
  private DatasetDistribution datasetDistribution;
  private DataPointerList batchPointers;
  private DataContents batchContents;

  /**
   * Creates a new streamed shuffle writer backend
   *
   * @param numPartitions number of reducer partitions for this shuffle
   * @param bufferedOutputStreams shared output streams that this shuffler will write the data into
   */
  public StreamedShuffleWriterBackend(int numPartitions, BufferedOutputStream[] bufferedOutputStreams) {
    this.numPartitions = numPartitions;
    this.bufferedOutputStreams = bufferedOutputStreams;
    this.batchPointers = new DataPointerList(BATCH_BUFFER_MAX_NUM);
    this.datasetDistribution = new DatasetDistribution(batchPointers, numPartitions);
    this.batchContents = new DataContents(datasetDistribution, batchPointers, BATCH_BUFFER_CAPACITY);
    assert(bufferedOutputStreams != null);
    assert(bufferedOutputStreams.length == numPartitions);
  }

  /**
   * Insert a record into the streamed shuffle writer
   *
   * @param buf The byte-array-represented data
   * @param serializedRecordSize The size of the byte array
   * @param partitionId The partition ID of the byte array
   * @throws IOException
   */
  public void insertRecord(byte[] buf, int serializedRecordSize, int partitionId) throws IOException {
    int numRecords = batchPointers.size();
    int contentBytes = batchContents.size();
    if (numRecords + 1 > BATCH_BUFFER_MAX_NUM || contentBytes + serializedRecordSize > BATCH_BUFFER_CAPACITY) {
      processBatch();
      batchPointers.clear();
      batchContents.clear();
    }
    int offset = batchContents.insert(buf, serializedRecordSize);
    batchPointers.insert(partitionId, offset, serializedRecordSize);
  }

  private void processBatch() throws IOException {
    datasetDistribution.computeDistribution(batchContents.size());
    batchPointers.sort(datasetDistribution);
    batchContents.sort();
    for (int i=0; i<numPartitions; i++) {
      byte[] buf = batchContents.getSortedBuf();
      int offset = datasetDistribution.getByteOffsetFromPartition(i);
      int bytes = datasetDistribution.getNumBytesFromPartition(i);
      bufferedOutputStreams[i].write(buf, offset, bytes);
    }
  }

  /**
   * Forcefully commit the buffered content into disk
   * @throws IOException
   */
  public void flush() throws IOException {
    processBatch();
    batchPointers.clear();
    batchContents.clear();
    for (int i=0; i<numPartitions; i++) {
      bufferedOutputStreams[i].flush();
    }
  }

  public static void main(String[] args) {
    try {
      String path_prefix = "D:\\tmp\\";
      int num_partitions = 1033;
      int bytes_per_partition = 1031*1024;
      byte[][] partitionToElements = new byte[num_partitions][];
      BufferedOutputStream[] outputStreams = new BufferedOutputStream[num_partitions];
      for(int i=0; i<num_partitions; i++) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putInt(i);
        partitionToElements[i] = bb.array();
        String path = path_prefix + "reduce_output_" + i;
        FileOutputStream fos = new FileOutputStream(path, false);
        BufferedOutputStream bos = new BufferedOutputStream(fos, 256*1024);
        outputStreams[i] = bos;
      }
      StreamedShuffleWriterBackend backend = new StreamedShuffleWriterBackend(num_partitions, outputStreams);
      int count = bytes_per_partition / 4;
      for(int i=0; i<count; i++) {
        for(int j=0; j<num_partitions; j++) {
          backend.insertRecord(partitionToElements[j], 4, j);
        }
      }
      backend.flush();
      byte[] array = new byte[4];
      for(int i=0; i<num_partitions; i++) {
        String path = path_prefix + "reduce_output_" + i;
        FileInputStream fis = new FileInputStream(path);
        BufferedInputStream bis = new BufferedInputStream(fis, 256*1024);
        for(int j=0; j<count; j++) {
          bis.read(array, 0, 4);
          ByteBuffer buf = ByteBuffer.wrap(array);
          int val = buf.getInt();
          if (val != i) {
            assert(false);
          }
        }
      }
      System.out.println("Hello, world!");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

