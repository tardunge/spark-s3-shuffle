//
// Copyright 2023- IBM Inc. All rights reserved
// SPDX-License-Identifier: Apache 2.0
//

package org.apache.spark.storage

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.helper.S3ShuffleDispatcher

import java.io.{BufferedInputStream, InputStream}
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

class S3BufferedPrefetchIterator(iter: Iterator[(BlockId, S3ShuffleBlockStream)], maxBufferSize: Long)
    extends Iterator[(BlockId, InputStream)]
    with Logging {
  
  // Timeout constants
  private val MEMORY_WAIT_TIMEOUT_MS = 5000
  private val NEXT_ELEMENT_TIMEOUT_SEC = 30
  
  private val startTime = System.nanoTime()

  @volatile private var memoryUsage: Long = 0
  @volatile private var hasItem: Boolean = iter.hasNext
  private var timeWaiting: Long = 0
  private var timePrefetching: Long = 0
  private var numStreams: Long = 0
  private var bytesRead: Long = 0

  // Use atomic counter for thread-safe updates
  private val activeTasks = new AtomicInteger(0)
  
  // Use LinkedBlockingDeque for thread-safe LIFO operations
  private val completed = new LinkedBlockingDeque[(InputStream, BlockId, Long)]()
  
  // Separate lock for memory management
  private val memoryLock = new Object()

  private class ThreadPredictor(maxThreads: Int) {
    private var currentThreads = 1
    private val latencies = Array.fill(maxThreads + 2)(0.toLong)
    latencies(0) = Long.MaxValue
    latencies(maxThreads + 1) = Long.MaxValue

    private var numMeasurements = 0
    private val measurementsNS = Array.fill(20)(0.toLong)

    private def predict(): Int = synchronized {
      if (numMeasurements < measurementsNS.length + currentThreads) {
        return currentThreads
      }
      val current = measurementsNS.sum
      if (current < 500) { // Less than 25ns latency for each request.
        return currentThreads
      }
      latencies(currentThreads) = current
      val prevValue = latencies(currentThreads - 1)
      val nextValue = latencies(currentThreads + 1)

      numMeasurements = 0
      if (prevValue < current) {
        currentThreads -= 1
      } else if (nextValue < current) {
        currentThreads += 1
      }
      currentThreads
    }

    def addMeasurementAndPredict(latencyNS: Long): Int = synchronized {
      if (latencyNS >= 0) {
        measurementsNS(numMeasurements % measurementsNS.length) = latencyNS
        numMeasurements += 1
      }
      predict()
    }
  }

  private val threadPredictor = new ThreadPredictor(S3ShuffleDispatcher.get.maxConcurrencyTask)

  private val ptr = this
  private val currentActiveThreads = new AtomicLong(0)
  private val desiredActiveThreads = new AtomicLong(0)

  // Configure the threads based on the wait time.
  private def configureThreads(latency: Long): Unit = synchronized {
    if (desiredActiveThreads.get() != currentActiveThreads.get()) {
      return
    }
    val nThreads = threadPredictor.addMeasurementAndPredict(latency)
    val activeThreads = desiredActiveThreads.getAndSet(nThreads)
    if (nThreads > activeThreads) {
      val t = new Thread {
        override def run(): Unit = {
          ptr.prefetchThread(nThreads)
        }
      }
      t.start()
    }
  }
  // Make sure that there's at least a single thread running.
  configureThreads(-1)

  private def onCloseStream(bufferSize: Int): Unit = {
    memoryLock.synchronized {
      memoryUsage -= bufferSize
      memoryLock.notifyAll()  // Wake up threads waiting for memory
    }
  }

  private def prefetchThread(threadId: Long): Unit = {
    currentActiveThreads.incrementAndGet()
    var nextElement: (BlockId, S3ShuffleBlockStream) = null
    
    while (true) {
      // Step 1: Get next work item (minimal lock time)
      val shouldExit = synchronized {
        if (!iter.hasNext && nextElement == null) {
          hasItem = false
          true  // exit
        } else if (nextElement == null) {
          if (threadId > desiredActiveThreads.get()) {
            currentActiveThreads.decrementAndGet()
            true  // exit
          } else {
            nextElement = iter.next()
            activeTasks.incrementAndGet()  // Increment when taking element
            hasItem = iter.hasNext
            false  // continue
          }
        } else {
          false  // continue - already have an element to retry
        }
      }
      
      if (shouldExit) {
        currentActiveThreads.decrementAndGet()
        return
      }
      
      // Step 2: Wait for memory (separate lock)
      val bsize = scala.math.min(maxBufferSize, nextElement._2.maxBytes).toInt
      var memoryAllocated = false
      
      memoryLock.synchronized {
        val deadline = System.currentTimeMillis() + MEMORY_WAIT_TIMEOUT_MS
        var timedOut = false
        while (memoryUsage + bsize > maxBufferSize && !timedOut) {
          val remaining = deadline - System.currentTimeMillis()
          if (remaining <= 0) {
            logWarning(s"Timeout waiting for memory allocation of $bsize bytes")
            // Continue anyway to avoid deadlock
            timedOut = true
          } else {
            try {
              memoryLock.wait(remaining)
            } catch {
              case _: InterruptedException =>
            }
          }
        }
        if (!timedOut) {
          memoryUsage += bsize
          memoryAllocated = true
        }
      }
      
      if (memoryAllocated) {
        // Step 3: Process (no lock needed)
        val block = nextElement._1
        val s = nextElement._2
        nextElement = null  // Clear for next iteration
        val now = System.nanoTime()
        val stream = new S3BufferedInputStreamAdaptor(s, bsize, onCloseStream)
        timePrefetching += System.nanoTime() - now
        bytesRead += bsize
        
        // Step 4: Add to completed queue (thread-safe, LIFO order)
        completed.addFirst((stream, block, bsize))
        activeTasks.decrementAndGet()  // Decrement only after completion
        
        // Notify waiting consumers
        synchronized {
          notifyAll()
        }
      } else {
        // Memory allocation timed out - process without buffer to prevent deadlock
        logWarning(s"Memory allocation timed out for block ${nextElement._1}. Processing without buffer to prevent deadlock.")
        val block = nextElement._1
        val s = nextElement._2  // Raw S3ShuffleBlockStream (already an InputStream)
        nextElement = null  // Clear for next iteration
        
        // Add raw stream directly without buffering (memory usage = 0)
        completed.addFirst((s, block, 0L))
        activeTasks.decrementAndGet()  // Decrement after completion
        
        // Notify waiting consumers
        synchronized {
          notifyAll()
        }
      }
    }
  }

  private def printStatistics(): Unit = synchronized {
    val totalRuntime = System.nanoTime() - startTime
    val tc = TaskContext.get()
    val sId = tc.stageId()
    val sAt = tc.stageAttemptNumber()
    try {
      val tR = totalRuntime / 1000000
      val wPer = 100 * timeWaiting / totalRuntime
      val tW = timeWaiting / 1000000
      val tP = timePrefetching / 1000000
      val bR = bytesRead
      val r = numStreams
      // Average time per prefetch
      val atP = tP / r
      // Average time waiting
      val atW = tW / r
      // Average read bandwidth
      val bW = bR.toDouble / (tP.toDouble / 1000) / (1024 * 1024)
      // Block size
      val bs = bR / r
      // Threads
      val ta = desiredActiveThreads.get()
      logInfo(
        s"Statistics: Stage ${sId}.${sAt} TID ${tc.taskAttemptId()} -- " +
          s"${bR} bytes, ${tW} ms waiting (${atW} avg), " +
          s"${tP} ms prefetching (avg: ${atP} ms - ${bs} block size - ${bW} MiB/s). " +
          s"Total: ${tR} ms - ${wPer}% waiting. ${ta} active threads."
      )
    } catch {
      case e: Exception => logError(f"Unable to print statistics: ${e.getMessage}.")
    }
  }

  override def hasNext: Boolean = {
    val result = hasItem || activeTasks.get() > 0 || !completed.isEmpty
    if (!result) {
      printStatistics()
    }
    result
  }

  override def next(): (BlockId, InputStream) = {
    val now = System.nanoTime()
    
    // Poll with timeout instead of synchronized wait
    var result: (InputStream, BlockId, Long) = null
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(NEXT_ELEMENT_TIMEOUT_SEC)
    
    while (result == null && System.currentTimeMillis() < deadline) {
      result = completed.pollFirst(100, TimeUnit.MILLISECONDS)
      if (result == null && !hasNext) {
        throw new NoSuchElementException("No more elements")
      }
    }
    
    if (result == null) {
      throw new java.util.concurrent.TimeoutException(s"Timeout waiting for next element after ${NEXT_ELEMENT_TIMEOUT_SEC} seconds")
    }
    
    val timeBetweenReads = System.nanoTime() - now
    timeWaiting += timeBetweenReads
    
    configureThreads(timeBetweenReads)
    numStreams += 1
    
    (result._2, result._1)
  }
}