/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark


import java.util.concurrent.{Executors, TimeUnit}

import org.scalatest.FunSuite

class SharedObjectManagerSuite extends FunSuite {
  test("shared state correctness") {
    val sharedObjectManager = new SharedObjectManager
    val executor = Executors.newFixedThreadPool(16)
    println("Submitting")
    for (i <- 0 until 1024*1024) {
      executor.submit(new Runnable {
        override def run(): Unit = {
          sharedObjectManager.getOrCreate(i, (x: Int) => 0)
        }
      })
    }
    println("Waiting")
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    println("Done")
    for (i <- 0 until 1024*1024) {
      assert(sharedObjectManager.get(i).asInstanceOf[Int] == 0)
    }
  }
}
