/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{BiPredicate, Function => JFunction, Predicate => JPredicate}


/**
  * Manages the objects that is shared among tasks inside the driver/executor.
  *
  */
private[spark] class SharedObjectManager {
  val sharedObjects = new ConcurrentHashMap[AnyRef, AnyRef]()

  def getOrCreate[K, V](k: K, create: K => V): V = {
    val javaFunction: JFunction[AnyRef, AnyRef] = SharedObjectManager.toJavaFunction((k: AnyRef) => create(k.asInstanceOf[K]).asInstanceOf[AnyRef])
    sharedObjects.computeIfAbsent(k.asInstanceOf[AnyRef], javaFunction).asInstanceOf[V]
  }

  def get[K](k: K): AnyRef = {
    sharedObjects.get(k)
  }
}

object SharedObjectManager {
  //usage example: `i: Int ⇒ 42`
  def toJavaFunction[A, B](f: Function1[A, B]) = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  //usage example: `i: Int ⇒ true`
  def toJavaPredicate[A](f: Function1[A, Boolean]) = new JPredicate[A] {
    override def test(a: A): Boolean = f(a)
  }

  //usage example: `(i: Int, s: String) ⇒ true`
  def toJavaBiPredicate[A, B](predicate: (A, B) ⇒ Boolean) =
    new BiPredicate[A, B] {
      def test(a: A, b: B) = predicate(a, b)
    }
}
