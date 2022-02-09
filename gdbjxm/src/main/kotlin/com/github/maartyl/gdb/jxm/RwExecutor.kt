package com.github.maartyl.gdb.jxm

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.launch

interface RwSlot {
  val readOnly: Boolean

  //must not throw
  // must be OK to run multiple times!
  //- it SUSPENDS UNTIL DONE running, but always represents the same run
  suspend fun startAndJoin()
}

//executes SLOTS
// - a WRITE slot only ever executes alone
// - multiple READ-ONLY slots can execute at the same time
class RwExecutor(scope: CoroutineScope) {

  //is the buffer even useful? probably does not hurt
  // - some explicit buffer will be needed, if I allow out-of-order (prioritized) execution - future
  private val queue = Channel<RwSlot>(100)

  suspend fun enqueue(slot: RwSlot) {
    queue.send(slot)
  }

  //for now a simple variant
  //future: also run CHEAP RO even if some RW in queue

  private val mgr = scope.launch {

    val myJob = currentCoroutineContext().job
    for (s in queue) {
      if (s.readOnly) { //those can run in parallel
        launch { s.startAndJoin() }
      } else {
        //wait for all children == readOnly
        myJob.children.forEach { it.join() }

        s.startAndJoin() //run rw sequentially
      }
    }
  }
}