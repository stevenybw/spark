/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.gpft

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * [[PerThreadCollectionPool]] is the key component of thread GpFT. It keeps track of a total-order timestamp counter
  * called "current epoch" (t) and a mapping from currently active task id (tid) to owned collection.
  *
  * It contains a pool of collections. Each collection with id i is associated with an a monotone-increasing last
  * committed epoch (q_i). Each collection with id i is also associated with an active task-ids (T_i), which is
  * the id of the tasks involved in the epoch q_i.
  *
  * When a task request an collection q_i from the pool, the ownership of q_i is transferred to this task and the
  * task is assigned with current epoch. If the current epoch t is larger than q_i, it will call "commit" of this
  * collection,update q_i and clear T_i. After a worker thread has finished, this thread will transfer the ownership
  * of the collection back to this pool. Multiple worker threads may concurrently fetch the collection from the pool.
  *
  * When a task failed, the executor knows the failed task id. The owned collection is checked from the mapping. The
  * "rollback" of this container will be called. Its associated active tasks will also be marked as failure.
  */
class PerThreadCollectionPool[T](sharedCounter: AtomicLong, createCollection: Long=>T) extends Logging {
  private val _currentEpoch = new AtomicLong(0)
  private val lock = new ReentrantLock()

  /** Total number of collections created using createCollection */
  private var _numCollections = 0

  /** From task id to collection */
  private val ownedCollectionMap = new mutable.HashMap[Long, T]()

  /** Collections available */
  private val collections = new mutable.Queue[T]()

  /**
    * Increase the epoch. Recent changes will be committed.
    * Thread safe
    */
  def increaseEpoch(): Long = _currentEpoch.incrementAndGet()

  /**
    * Get current epoch
    */
  def currentEpoch: Long = _currentEpoch.get()

  def numCollections(): Int = _numCollections

  /**
    * Pop a collection from this pool
    */
  def pop(): Option[T] = {
    popAndGetSize()._1
  }

  /**
    * Pop a collection from this pool and get the size of this pool after popping
    */
  def popAndGetSize(): (Option[T], Int) = {
    lock.lock()
    val result = if (collections.isEmpty) {
      None
    } else {
      Some(collections.dequeue())
    }
    val size = collections.size
    lock.unlock()
    (result, size)
  }

  /**
    * Borrow a collection from this pool to an active task
    *
    * REMINDER: A borrowed collection must be returned back by calling [[give]]
    */
  def borrow(tid: Long): T = {
    logInfo(s"task ${tid} attempts to borrow")
    lock.lock()
    val epoch = currentEpoch
    val collection = if (collections.isEmpty) {
      val collectionId = sharedCounter.getAndIncrement()
      _numCollections += 1
      val collection = createCollection(collectionId)
      val collectionAsRevertable = collection.asInstanceOf[Revertable]
      collectionAsRevertable.lastCommittedEpoch = epoch
      logInfo(s"task ${tid} creates a new collection with id ${collectionId} (num collections = ${_numCollections})")
      collection
    } else {
      logInfo(s"task ${tid} use existing collection (num collections = ${_numCollections})")
      collections.dequeue()
    }
    require(!ownedCollectionMap.contains(tid), "a task cannot borrow multiple times")
    ownedCollectionMap.put(tid, collection)
    val collectionAsRevertable = collection.asInstanceOf[Revertable]
    if (epoch > collectionAsRevertable.lastCommittedEpoch) {
      collectionAsRevertable.commit()
      collectionAsRevertable.lastCommittedEpoch = epoch
      collectionAsRevertable.activeTaskIds.clear()
    }
    lock.unlock()
    collection
  }

  /**
    * Give back a collection to this pool from a terminating active task
    */
  def give(tid: Long, collection: T): Unit = {
    lock.lock()
    require(ownedCollectionMap.contains(tid))
    ownedCollectionMap.remove(tid)
    collections.enqueue(collection)
    lock.unlock()
    logInfo(s"task ${tid} return a collection back")
  }

  /**
    * Fail a task group containing given active task id
    */
  def fail(tid: Long): Array[Long] = {
    lock.lock()
    val failedTasks = if (ownedCollectionMap.contains(tid)) {
      val collection = ownedCollectionMap(tid)
      val collectionAsRevertable = collection.asInstanceOf[Revertable]
      ownedCollectionMap.remove(tid)
      val failedTasks = collectionAsRevertable.activeTaskIds.toArray
      val epoch = currentEpoch
      collectionAsRevertable.rollback()
      collectionAsRevertable.lastCommittedEpoch = epoch
      collectionAsRevertable.activeTaskIds.clear()
      collections.enqueue(collection)
      failedTasks
    } else {
      Array(tid)
    }
    lock.unlock()
    failedTasks
  }
}
