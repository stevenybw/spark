/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle

import org.apache.spark.scheduler.MapStatus

/**
  * Obtained inside a shuffle flush task to write out buffered content to the shuffle system
  */
trait ShuffleFlusher {
  /** Write out the buffered records to the shuffle system */
  def flush(): MapStatus
}
