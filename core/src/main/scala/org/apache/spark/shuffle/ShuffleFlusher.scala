/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle

import org.apache.spark.scheduler.MapStatus

/**
  * Obtained inside a shuffle flush task to write out and release buffered content to the shuffle system
  * It's guaranteed that for each shuffle stage in a executor, flush will be called exactly once, and after called,
  * there will be no more tasks in this shuffle stage be submitted to this executor.
  */
trait ShuffleFlusher {
  /** Write out the buffered records to the shuffle system */
  def flush(): MapStatus
}
