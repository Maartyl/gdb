package com.github.maartyl.gdb.jxm

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

interface RwSlot : suspend CoroutineScope.() -> Unit {
  val readOnly: Boolean

  //must not throw
  // must be OK to run multiple times!
  //- it SUSPENDS UNTIL DONE running, but always represents the same run
  //suspend fun startAndJoin()
  // INSTEAD uses INVOKE
  //   and must IGNORE scope
}

//executes SLOTS
// - a WRITE slot only ever executes alone
// - multiple READ-ONLY slots can execute at the same time
class RwExecutor(scope: CoroutineScope) {

  //is the buffer even useful? probably does not hurt
  // - some explicit buffer will be needed, if I allow out-of-order (prioritized) execution - future
  // -- NOPE! correct way is to use different buffer for priority and the SELECT from both
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
        launch(start = CoroutineStart.UNDISPATCHED, block = s)
      } else {
        //wait for all children == readOnly
        myJob.children.forEach { it.join() }

        s.invoke(this) //run rw sequentially
      }
    }
  }
}