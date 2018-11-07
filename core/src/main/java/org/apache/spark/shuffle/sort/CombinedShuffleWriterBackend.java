/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.sort;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Java Backend for In-Memory Merging
 *
 */
public class CombinedShuffleWriterBackend {
  private static final int FANOUT_SIZE_BITS = 8;
  private static final int FANOUT_SIZE = (1 << FANOUT_SIZE_BITS);
  private static final int FANOUT_SIZE_MASK = (1 << FANOUT_SIZE_BITS) - 1;
  private static final int BATCH_BUFFER_MAX_NUM = 1 * 1024*1024;
  private static final int BATCH_BUFFER_CAPACITY = 16 * 1024*1024;

  /** DataEntry
   * Stores the partitionId of that data entry together with offset in the batch buffer
   */
  private class DataEntry {
    int partitionId;
    int offset;
    int size;
  }

  private int numPartitions;
  private BufferedOutputStream[] bufferedOutputStreams;

  private DataEntry[] dataEntries;
  private DataEntry[] sortedDataEntries;
  private int dataEntriesSize;
  private int dataEntriesCapacity;

  private byte[] batchBuffer;
  private byte[] sortedBatchBuffer;
  private int batchBufferSize;
  private int batchBufferCapacity;

  /** Number of data entries for each chunk */
  private int[] chunkToDataEntriesOffset;

  /** Partition id to offset in the batch buffer */
  private int[] partitionToOffset;
  private int[] partitionToBytes;

  /**
   *
   * @param numPartitions number of reducer partitions for this shuffle
   * @param bufferedOutputStreams shared output streams that this shuffler will write the data into
   */
  public CombinedShuffleWriterBackend(int numPartitions, BufferedOutputStream[] bufferedOutputStreams) {
    this.numPartitions = numPartitions;
    this.bufferedOutputStreams = bufferedOutputStreams;
    assert(bufferedOutputStreams != null);
    assert(bufferedOutputStreams.length == numPartitions);
    dataEntries = new DataEntry[BATCH_BUFFER_MAX_NUM];
    sortedDataEntries = new DataEntry[BATCH_BUFFER_MAX_NUM];
    for (int i=0; i<BATCH_BUFFER_MAX_NUM; i++) {
      dataEntries[i] = new DataEntry();
      sortedDataEntries[i] = new DataEntry();
    }
    dataEntriesSize = 0;
    dataEntriesCapacity = BATCH_BUFFER_MAX_NUM;
    batchBuffer = new byte[BATCH_BUFFER_CAPACITY];
    sortedBatchBuffer = new byte[BATCH_BUFFER_CAPACITY];
    batchBufferSize = 0;
    batchBufferCapacity = BATCH_BUFFER_CAPACITY;
    chunkToDataEntriesOffset = new int[FANOUT_SIZE];
    partitionToOffset = new int[numPartitions];
    partitionToBytes = new int[numPartitions];
  }

  /** Append a binary record into specified partition id*/
  public void insertRecord(byte[] buf, int serializedRecordSize, int partitionId) throws IOException {
    if (!(dataEntriesSize + 1 <= dataEntriesCapacity && batchBufferSize + serializedRecordSize <= batchBufferCapacity)) {
      processBatch();
      dataEntriesSize = 0;
      batchBufferSize = 0;
    }
    System.arraycopy(buf, 0, batchBuffer, batchBufferSize, serializedRecordSize);
    dataEntries[dataEntriesSize].partitionId = partitionId;
    dataEntries[dataEntriesSize].offset = batchBufferSize;
    dataEntries[dataEntriesSize].size = serializedRecordSize;
    dataEntriesSize++;
    batchBufferSize+=serializedRecordSize;
  }

  private void processBatch() throws IOException {
    int numPartitionsBits = Log2UpperBound(numPartitions);
    int currentOffset = 0;

    // Prepare for sorted data entries
    while (currentOffset < numPartitionsBits) {
      for (int i=0; i<FANOUT_SIZE; i++) {
        chunkToDataEntriesOffset[i] = 0;
      }
      for (int i=0; i<dataEntriesSize; i++) {
        int partition_id = dataEntries[i].partitionId;
        int chunk_id = (partition_id >> currentOffset) & FANOUT_SIZE_MASK;
        chunkToDataEntriesOffset[chunk_id]++;
      }
      int prefixSum = 0;
      for (int i=0; i<FANOUT_SIZE; i++) {
        int currentVal = chunkToDataEntriesOffset[i];
        chunkToDataEntriesOffset[i] = prefixSum;
        prefixSum += currentVal;
      }
      for (int i=0; i<dataEntriesSize; i++) {
        int partition_id = dataEntries[i].partitionId;
        int chunk_id = (partition_id >> currentOffset) & FANOUT_SIZE_MASK;
        int offset = chunkToDataEntriesOffset[chunk_id]++;
        sortedDataEntries[offset] = dataEntries[i];
      }
      assert(chunkToDataEntriesOffset[FANOUT_SIZE-1] == dataEntriesSize);
      DataEntry[] tmp = dataEntries;
      dataEntries = sortedDataEntries;
      sortedDataEntries = tmp;
      currentOffset += FANOUT_SIZE_BITS;
    }

    // Apply the data entries to batch buffer
    int nextBufferOffset = 0;
    int previousPartitionId = -1;
    for (int i=0; i<dataEntriesSize; i++) {
      int partitionId = dataEntries[i].partitionId;
      if (partitionId > previousPartitionId) {
        for(int j=previousPartitionId+1; j<=partitionId; j++) {
          partitionToOffset[j] = nextBufferOffset;
        }
        previousPartitionId = partitionId;
      }
      int offset = dataEntries[i].offset;
      int size = dataEntries[i].size;
      System.arraycopy(batchBuffer, offset, sortedBatchBuffer, nextBufferOffset, size);
      nextBufferOffset += size;
    }
    for (int j=previousPartitionId+1; j<numPartitions; j++) {
      partitionToOffset[j] = nextBufferOffset;
    }
    for (int i=0; i<numPartitions-1; i++) {
      partitionToBytes[i] = partitionToOffset[i+1] - partitionToOffset[i];
    }
    partitionToBytes[numPartitions - 1] = batchBufferSize - partitionToOffset[numPartitions - 1];

    // write back for each partition
    for (int i=0; i<numPartitions; i++) {
      bufferedOutputStreams[i].write(sortedBatchBuffer, partitionToOffset[i], partitionToBytes[i]);
    }
  }

  private int Log2UpperBound(int number) {
    int curr = number;
    int pow = 0;
    while(curr != 1) {
      curr >>= 1;
      pow++;
    }
    if ((1<<pow) < number) {
      return pow+1;
    } else {
      return pow;
    }
  }

  /** Forcefully write back the in-memory content */
  public void writeBack() throws IOException {
    processBatch();
    dataEntriesSize = 0;
    batchBufferSize = 0;
  }

  /** Flush the in-memory data structure into files */
  public void flush() throws IOException {
    writeBack();
    for (int i=0; i<numPartitions; i++) {
      bufferedOutputStreams[i].flush();
    }
  }

  public static void main(String[] args) {
    try {
      String path_prefix = "D:\\tmp\\";
      int num_partitions = 1024;
      int bytes_per_partition = 8*1024*1024;
      byte[][] partitionToElements = new byte[num_partitions][];
      BufferedOutputStream[] outputStreams = new BufferedOutputStream[num_partitions];
      for(int i=0; i<num_partitions; i++) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(i);
        partitionToElements[i] = bb.array();
        String path = path_prefix + "reduce_output_" + i;
        FileOutputStream fos = new FileOutputStream(path, false);
        BufferedOutputStream bos = new BufferedOutputStream(fos, 256*1024);
        outputStreams[i] = bos;
      }
      CombinedShuffleWriterBackend backend = new CombinedShuffleWriterBackend(num_partitions, outputStreams);
      int count = bytes_per_partition / 8;
      for(int i=0; i<count; i++) {
        for(int j=0; j<num_partitions; j++) {
          backend.insertRecord(partitionToElements[j], 4, j);
        }
      }
      backend.flush();
      System.out.println("Hello, world!");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
