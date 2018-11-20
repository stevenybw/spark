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

/**
 * Data distribution used by radix sort to determine writing position for each chunk / partition
 *
 * A naive method would requires a scanning for each chunk. However, we can computes the histogram for each
 * iteration in one scanning to avoid extra scanning. This is possible because the count distribution for each
 * chunk is invariable to permutation.
 */
public class DatasetDistribution {
  DataPointerList batchEntries;
  private int numPartitions;
  private int[][] pointerOffsetByIterChunk; // [maxIter][numChunks]
  private int[] pointerOffsetByPartition; // [numPartitions]
  private int[] bytesOffsetByPartition; // [numPartitions]
  private int[] clonedBytesOffsetByPartition; // [numPartitions]

  /**
   * Construct a dataset distribution without computing. To compute, use computeDistribution.
   * @param batchEntries associated batchEntries
   * @param numPartitions number of partitions of the records
   */
  DatasetDistribution(DataPointerList batchEntries, int numPartitions) {
    this.batchEntries = batchEntries;
    this.numPartitions = numPartitions;
    pointerOffsetByIterChunk = new int[3][];
    for(int i=0; i<3; i++) {
      pointerOffsetByIterChunk[i] = new int[256];
    }
    pointerOffsetByPartition = new int[numPartitions+1];
    bytesOffsetByPartition = new int[numPartitions+1];
    clonedBytesOffsetByPartition = new int[numPartitions+1];
  }

  /**
   * Erase the previous result and compute the elements distribution for the given dataset
   * @param totalBytes total number of bytes of contents
   */
  public void computeDistribution(int totalBytes) {
    int numElements = batchEntries.size();
    for(int i=0; i<3; i++) {
      for(int j=0; j<256; j++) {
        pointerOffsetByIterChunk[i][j] = 0;
      }
    }
    for(int i=0; i<numPartitions; i++) {
      pointerOffsetByPartition[i] = 0;
      bytesOffsetByPartition[i] = 0;
    }
    for(int i=0; i<numElements; i++) {
      int partitionId = batchEntries.getPartitionIdFromRecord(i);
      int recordSize = batchEntries.getRecordSizeFromId(i);
      for(int j=0; j<3; j++) {
        int chunkId = (partitionId >> (j*8)) & 0xFF;
        pointerOffsetByIterChunk[j][chunkId]++;
      }
      pointerOffsetByPartition[partitionId]++;
      bytesOffsetByPartition[partitionId] += recordSize;
    }
    for(int i=0; i<3; i++) {
      int sum = convertCountToPrefixSum(pointerOffsetByIterChunk[i], 256);
      assert(sum == numElements);
    }
    {
      int sum = convertCountToPrefixSum(pointerOffsetByPartition, numPartitions);
      assert(sum == numElements);
      pointerOffsetByPartition[numPartitions] = numElements;
    }
    {
      int sum = convertCountToPrefixSum(bytesOffsetByPartition, numPartitions);
      assert(sum == totalBytes);
      bytesOffsetByPartition[numPartitions] = totalBytes;
    }
  }

  private static int convertCountToPrefixSum(int[] array, int size) {
    int prefixSum = 0;
    for(int i=0; i<size; i++) {
      int value = array[i];
      array[i] = prefixSum;
      prefixSum += value;
    }
    return prefixSum;
  }

  /**
   * Get the byte offset of specified partition
   * @param partitionId the partition id
   * @return the byte offset of specified partition
   */
  public int getByteOffsetFromPartition(int partitionId) {
    return bytesOffsetByPartition[partitionId];
  }

  /**
   * Get the number of bytes of specified partition
   * @param partitionId the partition id
   * @return the num bytes of specified partition
   */
  public int getNumBytesFromPartition(int partitionId) {
    return bytesOffsetByPartition[partitionId+1] - bytesOffsetByPartition[partitionId];
  }

  /**
   * Copy current partition offset to a new array (which is further used by getPartitionOffsetAndIncrease
   */
  public void copyPartitionOffset() {
    System.arraycopy(bytesOffsetByPartition, 0, clonedBytesOffsetByPartition, 0, numPartitions+1);
  }

  /**
   * Get the byte offset of specified partition and increase the offset up record size
   *
   * <b>Caution: this will destroy the statistics</b>
   * @param partitionId partition id
   * @param size the size to increment
   * @return original partition offset
   */
  public int getPartitionOffsetAndIncrease(int partitionId, int size) {
    int value = clonedBytesOffsetByPartition[partitionId];
    clonedBytesOffsetByPartition[partitionId] += size;
    return value;
  }

  /**
   * Get the pointer offset
   * @param iter iteration number
   * @param chunkId chunk id
   * @return the pointer offset
   */
  public int getPointerOffsetByIterChunkAndIncrement(int iter, int chunkId) {
    return pointerOffsetByIterChunk[iter][chunkId]++;
  }

}
