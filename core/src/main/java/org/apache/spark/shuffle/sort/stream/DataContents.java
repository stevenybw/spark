/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.sort.stream;

public class DataContents {
  private DatasetDistribution batchDistribution;
  private DataPointerList batchEntries;
  private int size;
  private byte[] buffer;
  private byte[] sortedBuffer;

  /**
   * Constructor
   */
  public DataContents(DatasetDistribution datasetDistribution, DataPointerList batchPointers, int batchBufferCapacity) {
    this.batchDistribution = datasetDistribution;
    this.batchEntries = batchPointers;
    this.buffer = new byte[batchBufferCapacity];
    this.sortedBuffer = new byte[batchBufferCapacity];
  }

  /**
   *
   * @return the number of bytes in this content array
   */
  public int size() {
    return size;
  }

  /**
   * Clear the buffer
   */
  public void clear() {
    size = 0;
  }

  /**
   * Append a record into this buffer
   * @param buf the record
   * @param numBytes the bytes of the record
   * @return the byte offset of the record inserted
   */
  public int insert(byte[] buf, int numBytes) {
    System.arraycopy(buf, 0, buffer, size, numBytes);
    int offset = size;
    size += numBytes;
    return offset;
  }

  /**
   * Get the buffer for specified partition
   * @return
   */
  public byte[] getSortedBuf() {
    return sortedBuffer;
  }

  /**
   * Sort the data contents according to the pointers
   */
  public void sort() {
    int numRecords = batchEntries.size();
    batchDistribution.copyPartitionOffset();
    for(int i=0; i<numRecords; i++) {
      int partitionId = batchEntries.getPartitionIdFromSortedRecord(i);
      int recordOffset = batchEntries.getSortedRecordOffsetFromId(i);
      int recordSize = batchEntries.getSortedRecordSizeFromId(i);
      int recordWriteOffset = batchDistribution.getPartitionOffsetAndIncrease(partitionId, recordSize);
      System.arraycopy(buffer, recordOffset, sortedBuffer, recordWriteOffset, recordSize);
    }
  }
}
