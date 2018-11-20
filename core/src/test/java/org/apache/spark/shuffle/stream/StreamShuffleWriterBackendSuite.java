/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

/*
 * Author: Bowen Yu <stevenybw@hotmail.com>
 */

package org.apache.spark.shuffle.stream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class StreamShuffleWriterBackendSuite {
  private static final String path_prefix = "D:\\tmp\\";
  private static final int FINE_GRAINED_BYTES = 16;
  private static final int COARSE_GRAINED_BYTES = 256;
  private static final int TEST_NUM_PARTITIONS = 1033;
  private static final int TEST_PARTITION_BYTES = 417*1024;

  /**
   *
   * @param testCaseName the name to be displayed
   * @param batchBufferMaxNum num capacity for batch buffer
   * @param batchBufferCapacity bytes capacity for batch buffer
   * @param elementBytes shuffle granularity: finer granularity leads to larger overhead
   * @param numPartitions total number of output partitions
   * @param countsPerPartition total counts per partition
   * @param writeFile if write the result into file system
   */
  public void doShuffleBenchmark(String testCaseName, int batchBufferMaxNum, int batchBufferCapacity, int elementBytes, int numPartitions, int countsPerPartition, boolean writeFile) {
    assert(elementBytes >= 4);
    try {
      byte[][] partitionToElements = new byte[numPartitions][];
      BufferedOutputStream[] outputStreams = new BufferedOutputStream[numPartitions];
      if (!writeFile) {
        outputStreams = null;
      }
      for(int i = 0; i< numPartitions; i++) {
        ByteBuffer bb = ByteBuffer.allocate(elementBytes);
        bb.putInt(i);
        for(int j=4; j<elementBytes; j++) {
          bb.put((byte)0xFF);
        }
        partitionToElements[i] = bb.array();
        if (writeFile) {
          String path = path_prefix + "reduce_output_" + i;
          FileOutputStream fos = new FileOutputStream(path, false);
          BufferedOutputStream bos = new BufferedOutputStream(fos, 256 * 1024);
          outputStreams[i] = bos;
        }
      }

      long durationNano = -System.nanoTime();
      StreamedShuffleWriterBackend backend = new StreamedShuffleWriterBackend(numPartitions, outputStreams, batchBufferMaxNum, batchBufferCapacity);
      for(int i=0; i<countsPerPartition; i++) {
        for(int j = 0; j< numPartitions; j++) {
          backend.insertRecord(partitionToElements[j], elementBytes, j);
        }
      }
      backend.flush();
      durationNano += System.nanoTime();
      double totalBytes = numPartitions * countsPerPartition * elementBytes;
      System.out.println(String.format("Test: case %s: %.2f MB/s", testCaseName, 1e3*totalBytes/durationNano));

      // verify only if we write to file
      if (writeFile) {
        byte[] array = new byte[elementBytes];
        for (int i = 0; i < numPartitions; i++) {
          String path = path_prefix + "reduce_output_" + i;
          FileInputStream fis = new FileInputStream(path);
          BufferedInputStream bis = new BufferedInputStream(fis, 256 * 1024);
          for (int j = 0; j < countsPerPartition; j++) {
            bis.read(array, 0, elementBytes);
            ByteBuffer buf = ByteBuffer.wrap(array);
            int val = buf.getInt();
            assertEquals(val, i);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setup() {
    doShuffleBenchmark("initialize", 1024, 1024,
            FINE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/FINE_GRAINED_BYTES, false);
  }

  @Test
  public void fineGrainedSmallBatchBufferMem() {
    doShuffleBenchmark("fineGrainedSmallBatchBuffer", 1024, 1024,
            FINE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/FINE_GRAINED_BYTES, false);
  }

  @Test
  public void fineGrainedMediumBatchBufferMem() {
    doShuffleBenchmark("fineGrainedMediumBatchBuffer", 1024*1024, 1024*1024,
            FINE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/FINE_GRAINED_BYTES, false);
  }

  @Test
  public void coarseGrainedSmallBatchBufferMem() {
    doShuffleBenchmark("coarseGrainedSmallBatchBuffer", 1024, 1024,
            COARSE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/COARSE_GRAINED_BYTES, false);
  }

  @Test
  public void coarseGrainedMediumBatchBufferMem() {
    doShuffleBenchmark("coarseGrainedMediumBatchBuffer", 1024*1024, 1024*1024,
            COARSE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/COARSE_GRAINED_BYTES, false);
  }

  @Test
  @Ignore
  public void fineGrainedSmallBatchBufferFile() {
    doShuffleBenchmark("fineGrainedSmallBatchBufferFile", 1024, 1024,
            FINE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/FINE_GRAINED_BYTES, true);
  }

  @Test
  @Ignore
  public void fineGrainedMediumBatchBufferFile() {
    doShuffleBenchmark("fineGrainedMediumBatchBufferFile", 1024*1024, 1024*1024,
            FINE_GRAINED_BYTES, TEST_NUM_PARTITIONS, TEST_PARTITION_BYTES/FINE_GRAINED_BYTES, true);
  }
}
