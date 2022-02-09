package com.github.maartyl.gdb.jxm

import com.github.maartyl.gdb.NodeBase
import kotlinx.coroutines.channels.SendChannel

internal class SubscriptionRef<TR : NodeBase>(
  val g: GDbImpl,
  //emits must NOT suspend - instead, only keep latest
  private val emits: SendChannel<TR?>,
  private val ref: Ref<TR>,
) : ChangeObserver {

  //part of Tx: no need to synchronize
  private var changeSeen = false
  private var changedTo: TR? = null
  private fun reset() {
    changeSeen = false
    changedTo = null
  }

  override fun <TN : NodeBase> onNodeChanged(tx: TxImpl, ref: Ref<TN>, old: TN?, new: TN?) {
    @Suppress("UNCHECKED_CAST")
    if (this.ref.id == ref.id) {
      changeSeen = true
      changedTo = new as TR?
    }
  }

  override fun txPostCommit(tx: SnapImpl) {
    //also: even if changed multiple times in tx: will always emit (only) the latest value
    if (changeSeen) {
      //must not suspend (DROP_OLDEST)
      // if closed fine: will be unregistered soon
      emits.trySend(changedTo)
      reset()
    }
  }

  override fun txPostRollback(tx: SnapImpl) {
    reset()
  }
}