/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.sort.stream;

public class DataPointerList {
  private long[] pointerList;
  private long[] sortedPointerList;
  private long[] tempPointerList;
  private int[] recordSizes;
  private int[] sortedRecordSizes;
  private int[] tempRecordSizes;
  private int numEntries = 0;
  private int byteOffset = 0;

  public DataPointerList(int capacity) {
    pointerList = new long[capacity];
    sortedPointerList = new long[capacity];
    tempPointerList = new long[capacity];
    recordSizes = new int[capacity];
    sortedRecordSizes = new int[capacity];
    tempRecordSizes = new int[capacity];
  }

  private static long packPointer(int partitionId, int byteOffset) {
    return (((long)partitionId)<<32) + byteOffset;
  }

  private static int partitionIdFromPackedPointer(long pointer) {
    return (int)(pointer >>> 32);
  }

  private static int byteOffsetFromPackedPointer(long pointer) {
    return (int)(pointer & 0xFFFFFFFFL);
  }

  /**
   * @return the number of pointers in this list
   */
  public int size() {
    return numEntries;
  }

  /**
   * Clear the list
   */
  public void clear() {
    numEntries = 0;
    byteOffset = 0;
  }

  /**
   * Insert a record to this list
   *
   * @param partitionId partition id of the record
   * @param offset byte offset of the record
   * @param recordBytes size of the record
   */
  public void insert(int partitionId, int offset, int recordBytes) {
    assert(offset == byteOffset);
    pointerList[numEntries] = packPointer(partitionId, byteOffset);
    recordSizes[numEntries] = recordBytes;
    numEntries++;
    byteOffset+=recordBytes;
  }

  private void radixSortOnePass(DatasetDistribution dist, long[] srcList, int[] srcSizes, long[] dstList, int[] dstSizes, int iter) {
    for(int i=0; i<numEntries; i++) {
      long cp = srcList[i];
      int sz = srcSizes[i];
      int pid = partitionIdFromPackedPointer(cp);
      int cid = (pid>>>(8*iter))&0xFF;
      int offset = dist.getPointerOffsetByIterChunkAndIncrement(iter, cid);
      dstList[offset] = cp;
      dstSizes[offset] = sz;
    }
  }

  /**
   * Sort the list (original order is kept in order to)
   */
  public void sort(DatasetDistribution dist) {
    radixSortOnePass(dist, pointerList, recordSizes, sortedPointerList, sortedRecordSizes, 0);
    radixSortOnePass(dist, sortedPointerList, sortedRecordSizes, tempPointerList, tempRecordSizes, 1);
    radixSortOnePass(dist, tempPointerList, tempRecordSizes, sortedPointerList, sortedRecordSizes, 2);
  }

  /**
   * Get the partition id of a record
   * @param recordId the id of the record
   * @return the partition id
   */
  public int getPartitionIdFromRecord(int recordId) {
    return partitionIdFromPackedPointer(pointerList[recordId]);
  }

  /**
   * Get the size of a record
   * @param recordId record id
   * @return the number of bytes of this record
   */
  public int getRecordSizeFromId(int recordId) {
    return recordSizes[recordId];
  }

  /**
   * Get the data of this list
   * @return an array of pointers
   */
  // public long[] data() {
  // }

  /**
   * Get the partition id from ith sorted records
   * @param recordId the id after sort
   * @return
   */
  public int getPartitionIdFromSortedRecord(int recordId) {
    return partitionIdFromPackedPointer(sortedPointerList[recordId]);
  }

  /**
   * Get the bytes offset from ith sorted records
   * @param recordId the id after sort
   * @return the bytes offset
   */
  public int getSortedRecordOffsetFromId(int recordId) {
    return byteOffsetFromPackedPointer(sortedPointerList[recordId]);
  }

  /**
   * Get the record size of ith sorted record
   * @param recordId the id after sort
   * @return
   */
  public int getSortedRecordSizeFromId(int recordId) {
    return recordSizes[recordId];
  }
}
