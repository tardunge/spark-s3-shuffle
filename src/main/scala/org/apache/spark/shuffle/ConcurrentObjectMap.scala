//
// Copyright 2022- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.shuffle

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.util.Try

class ConcurrentObjectMap[K, V] {
  // Use ConcurrentHashMap for thread-safe storage
  private val map = new ConcurrentHashMap[K, V]()
  private val keyLocks = new ConcurrentHashMap[K, AnyRef]()
  
  def clear(): Unit = {
    map.clear()
    keyLocks.clear()
  }
  
  def getOrElsePut(key: K, op: K => V): V = {
    // Get or create lock for this specific key
    val lock = keyLocks.computeIfAbsent(key, _ => new Object())
    lock.synchronized {
      Option(map.get(key)).getOrElse {
        val value = op(key)
        map.put(key, value)
        value
      }
    }
  }
  
  def remove(filter: K => Boolean, action: Option[V => Unit]): Unit = {
    // Collect keys first without holding any locks
    val keysToRemove = map.keySet().asScala.filter(filter).toList
    
    // Process each key with its own lock
    keysToRemove.foreach { key =>
      Option(keyLocks.get(key)).foreach { lock =>
        lock.synchronized {
          Option(map.remove(key)).foreach { value =>
            keyLocks.remove(key)
            // Execute action if provided
            action.foreach(a => Try(a(value)))
          }
        }
      }
    }
  }
}