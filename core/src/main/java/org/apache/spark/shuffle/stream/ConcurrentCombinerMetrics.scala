/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

/** Metrics for a single task */
class ConcurrentCombinerMetrics {
  var recordsWritten = 0L
  var writeDuration = 0L
  var serializationDuration = 0L
  var bytesWritten = 0L
}
