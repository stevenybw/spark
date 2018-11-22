/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

/**
  * A [[BufferedConsumer]] has in-memory buffer
  */
private[spark] trait BufferedConsumer {
  /**
    * Close the consumer and release the resources
    */
  def close(): Unit

  /**
    * Flush the buffered content into downstream
    */
  def flush(): Unit
}
