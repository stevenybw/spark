/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream;

import java.io.*;

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
  public static final int DEFAULT_BATCH_BUFFER_MAX_NUM = 1 * 1024*1024;
  public static final int DEFAULT_BATCH_BUFFER_CAPACITY = 16 * 1024*1024;

  private int batchBufferMaxNum;
  private int batchBufferCapacity;

  // Batch states
  private int numPartitions;
  private BufferedOutputStream[] bufferedOutputStreams;
  private DatasetDistribution datasetDistribution;
  private DataPointerList batchPointers;
  private DataContents batchContents;

  public StreamedShuffleWriterBackend(int numPartitions, BufferedOutputStream[] bufferedOutputStreams) {
    this(numPartitions, bufferedOutputStreams, DEFAULT_BATCH_BUFFER_MAX_NUM, DEFAULT_BATCH_BUFFER_CAPACITY);
  }

  /**
   * Creates a new streamed shuffle writer backend
   *
   * @param numPartitions number of reducer partitions for this shuffle
   * @param bufferedOutputStreams shared output streams that this shuffler will write the data into
   */
  public StreamedShuffleWriterBackend(int numPartitions,
                                      BufferedOutputStream[] bufferedOutputStreams,
                                      int batchBufferMaxNum,
                                      int batchBufferCapacity) {
    this.batchBufferMaxNum = batchBufferMaxNum;
    this.batchBufferCapacity = batchBufferCapacity;
    this.numPartitions = numPartitions;
    this.bufferedOutputStreams = bufferedOutputStreams;
    this.batchPointers = new DataPointerList(batchBufferMaxNum);
    this.datasetDistribution = new DatasetDistribution(batchPointers, numPartitions);
    this.batchContents = new DataContents(datasetDistribution, batchPointers, batchBufferCapacity);
    if (bufferedOutputStreams != null) {
      assert (bufferedOutputStreams.length == numPartitions);
    }
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
    if (numRecords + 1 > batchBufferMaxNum || contentBytes + serializedRecordSize > batchBufferCapacity) {
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
    if (bufferedOutputStreams != null) {
      for (int i = 0; i < numPartitions; i++) {
        byte[] buf = batchContents.getSortedBuf();
        int offset = datasetDistribution.getByteOffsetFromPartition(i);
        int bytes = datasetDistribution.getNumBytesFromPartition(i);
        bufferedOutputStreams[i].write(buf, offset, bytes);
      }
    }
  }

  /**
   * Flush the data into underlying buffered output streams. But we deliberately not flush the file buffer to
   * combine the write.
   * @throws IOException
   */
  public void flush() throws IOException {
    processBatch();
    batchPointers.clear();
    batchContents.clear();
  }
}

