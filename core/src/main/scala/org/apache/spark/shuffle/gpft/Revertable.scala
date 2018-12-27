/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.gpft

import scala.collection.mutable

trait Revertable {
  var lastCommittedEpoch: Long = 0
  val activeTaskIds = new mutable.HashSet[Long]()

  /** Commit the recent changes */
  def commit(): Unit

  /** Discard the recent changes since last commit */
  def rollback(): Unit
}
