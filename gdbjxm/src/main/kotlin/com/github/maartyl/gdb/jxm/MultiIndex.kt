package com.github.maartyl.gdb.jxm

import com.github.maartyl.gdb.GDbSnap
import com.github.maartyl.gdb.GRangeIndex
import com.github.maartyl.gdb.GRef
import com.github.maartyl.gdb.NodeBase
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import org.mapdb.serializer.GroupSerializerObjectArray

//private val seriTupleBytesStr = SerializerArrayTuple(Serializer.BYTE_ARRAY, Serializer.STRING)
//private val seriTupleStrStr = SerializerArrayTuple(Serializer.STRING, Serializer.STRING)
//private val seriTupleLongStr = SerializerArrayTuple(Serializer.LONG, Serializer.STRING)
private val seriTupleStrStr = SerializerKV<String>(Serializer.STRING, Serializer.STRING)
private val seriTupleLongStr = SerializerKV<Long>(Serializer.LONG, Serializer.STRING)


internal fun <Key : Any, Node : NodeBase> multiIndexStr(
  g: GDbImpl,
  name: String,
  view: (GRef<*>, NodeBase, MutableCollection<Key>) -> Unit?,
  seri: (Key) -> String,
): MultiIndex<Key, String, Node> = MultiIndex(g, name, view, seri, seriTupleStrStr)

internal fun <Key : Any, Node : NodeBase> multiIndexLong(
  g: GDbImpl,
  name: String,
  view: (GRef<*>, NodeBase, MutableCollection<Key>) -> Unit?,
  seri: (Key) -> Long,
): MultiIndex<Key, Long, Node> = MultiIndex(g, name, view, seri, seriTupleLongStr)


// k == seri(Key), ref == r.id
// KeyB must have well-defined equals + Comparable
// cmpBias is for RANGE queries (== all searching in MULTIMAP) - only applies to KEY
// - allows to insert values "around" / "between" possible real values
// - this obj itself is NOT comparable in arbitrary contexts
// - is part of MULTIMAP impl
internal data class KR<KeyB : Comparable<KeyB>>(val k: KeyB, val ref: String, val cmpBias: Byte = 0)

//todo MultiMap - reverse and group indexes, possibly more
internal class MultiIndex<Key : Any, KeyB : Comparable<KeyB>, Node : NodeBase>(
  val g: GDbImpl,
  override val name: String,
  val view: (GRef<*>, NodeBase, MutableCollection<Key>) -> Unit?,
  val seri: (Key) -> KeyB,
  tupleSerializer: GroupSerializer<KR<KeyB>>,
) : ChangeObserver, GRangeIndex<Key, Node> {

  //TODO: delta packing tuples, since key can repeat many times ?
  // - probably not that many times - not worth the slow-down for now ...?
  //seriTupleBytesStr
  private val multimap = g.rw.treeSet("${C.PX_IDX}$name", tupleSerializer)
    //.counterEnable()
    .createOrOpen()

  private fun bs(k: Key) = seri(k)

  private fun pack(k: Key, ref: Ref<*>) = KR<KeyB>(bs(k), ref.id)

  private val txToAdd = mutableSetOf<KR<KeyB>>()
  private val txToRemove = mutableSetOf<KR<KeyB>>()
  override fun txPreCommit(tx: TxImpl) {
    val any = synchronized(this@MultiIndex) { txToAdd.isNotEmpty() || txToRemove.isNotEmpty() }
    if (any) //TODO: one day, this will probably be taking a smaller scope, but for now fine
      tx.scopeMut.launch(g.ioDispatcher) {
        synchronized(this@MultiIndex) {
          multimap.addAll(txToAdd)
          multimap.removeAll(txToRemove)
          txToAdd.clear()
          txToRemove.clear()
        }
      }
  }

  //ASSUMES synchronized
  private fun add(k: Key, ref: Ref<*>) /*= synchronized(this)*/ {
    val p = pack(k, ref)
    txToRemove.remove(p) //if previous change in this tx was removing it - cancel
    txToAdd.add(p)
    //btw. another node cannot remove this by accident: the ref "owns" this entry
    // (as no other node could have created it)
  }

  //ASSUMES synchronized
  private fun remove(k: Key, ref: Ref<*>) /*= synchronized(this)*/ {
    val p = pack(k, ref)
    txToAdd.remove(p) //if previous change in this tx was adding it - cancel
    txToRemove.add(p)
  }

  override fun <TN : NodeBase> onNodeChanged(tx: TxImpl, ref: Ref<TN>, old: TN?, new: TN?) {
    //TODO: am I allowed to call equals ? - Yes, but probably not worth it.
    // this check also (mainly) gets rid of BOTH null
    if (old === new) return

    val oldFwd = mutableSetOf<Key>()
    val newFwd = mutableSetOf<Key>()

    //if they MAY ever contribute to index
    var oldMay = true
    var newMay = true

    old?.let { oldMay = null != view(ref, it, oldFwd) }
    new?.let { newMay = null != view(ref, it, newFwd) }

    //conditions where no indexing done:
    // - if ANY node requires false for the same ref, then NONE are required to be indexed
    if (!oldMay || !newMay) return

    synchronized(this) {
      if (oldFwd.isNotEmpty())
        for (ok in oldFwd) {
          if (ok !in newFwd)
            remove(ok, ref)
        }
      if (newFwd.isNotEmpty())
        for (nk in newFwd) {
          if (nk !in oldFwd)
            add(nk, ref)
        }
    }
  }

  //TODO: notify INDEX key CHANGE to g -- for SUBSCRIBE
  // onIndexChanged(name, key)
  // MAYBE - also jsut notify: index used with the same onIndexUsed(name, key)
  // - ideally serialized key, so they are easier to compare, without worrying


  // --------------

  override fun find(snap: GDbSnap, key: Key): Flow<GRef<Node>> {
    //TODO: add BUFFER param for OPTIMIZATION of "want many = use buffer" vs "want 'single' = don't load more ahead"
    // AND if I do the "replay" or "caching" - option to SKIP that if expects TOO MANY
    // - maybe use "buffer = Infinite" to disable that ? ... well, no ...buffer is something else...
    val b = bs(key)
    val snp = snap as SnapImpl
    snp.scopeSlot.ensureActive()
    //TODO: setup cache?
    //  - MutableList that becomes source for key
    //  - becomes usable once the flow fully consumed
    //  - valid until index changes
    //  - HA!! SharedFlow auto solves this!
    //    TODO: add to MAP in Snap so other invokes with the same key SHARE the same SharedFlow

    //TODO: add to SEEN in snap

    //FIXME:   DAMMIT ... SharedFlow actually WRONG ...
    // - as it is a CHILD of the slot, unless read to END launch will NEVER COMPLETE
    // -- even if flow collect gets cancelled...
    // -- and snap-scope will NEVER complete ...
    // - EITHER I cannot use this ... or I have to CANCEL the launch somehow
    // -- maybe at the end of Snap ?
    // --- maybe cancel ALL children, after Snap "body" completes?

    //TODO: if i have "unlimited" replay: can just share the same Flow for ALL invokes ... (with the same key)
//    MutableSharedFlow<GRef<Node>>(replay = Int.MAX_VALUE).also { msf ->
//      snp.scopeSlot.launch(snp.g.ioDispatcher) {
//        val matchSet = multimap.subSet(KR(b, "", -1), KR(b, "", 1))
//        for (kr in matchSet) {
//          msf.emit(g.internRef(kr.ref))
//        }
//      }
//    }//.also { return it }

    //TODO: this variant is NOT checking snp.scopeSlot.ensureActive()
    //TODO: OPTIMIZE the buffer, if I go with this impl for a longer time
    val matchSet = multimap.subSet(KR(b, "", -1), KR(b, "", 1))
    return matchSet.asFlow().map { g.internRef<Node>(it.ref) }.flowOn(snp.g.ioDispatcher)

//    return flow {
//      snp.scopeSlot.ensureActive()
//      val matchSet = multimap.subSet(KR(b, "", -1), KR(b, "", 1))
//      matchSet.asFlow()
//      for (kr in matchSet) {
//        emit(g.internRef<Node>(kr.ref))
//        snp.scopeSlot.ensureActive()
//      }
//    }.flowOn(snp.g.ioDispatcher)

//    val matchSet = multimap.subSet(KR(b, "", -1), KR(b, "", 1))
//    return matchSet.asSequence().map { g.internRef(it.ref) }
  }

}


private class SerializerKV<KeyB : Comparable<KeyB>>(
  val serK: Serializer<KeyB>,
  val serR: Serializer<String>,
) : GroupSerializerObjectArray<KR<KeyB>>() {

  override fun deserialize(input: DataInput2, available: Int): KR<KeyB> {
    return KR(serK.deserialize(input, available), serR.deserialize(input, available))
  }

  override fun serialize(out: DataOutput2, value: KR<KeyB>) {
    serK.serialize(out, value.k)
    serR.serialize(out, value.ref)
  }

  override fun isTrusted(): Boolean {
    return serK.isTrusted && serR.isTrusted
  }

  override fun compare(first: KR<KeyB>, second: KR<KeyB>): Int {
    val ck = first.k.compareTo(second.k)
    if (ck != 0) return ck

    //ref == values are NEVER (?) compared
    // - if they return equal ... is it a problem? I don't care about their order...
    // ! the TREE might require absolute order, and assume equal keys are equal
    // - actually: DEFINITELY needed: how else would it efficiently know, if it already stored it or not?

    val cb = first.cmpBias.compareTo(second.cmpBias)
    if (cb != 0) return cb

    //better SAFE than SORRY
    return first.ref.compareTo(second.ref)
  }

  //tuple impl that worked with  subSet(arrayOf<Any?>(b), arrayOf<Any?>(b, null))
  //
//  fun compare(o1: Array<Any?>, o2: Array<Any?>): Int {
//    val len = Math.min(o1.size, o2.size)
//    for (i in 0 until len) {
//      val a1 = o1[i]
//      val a2 = o2[i]
//      if (a1 === a2) continue
//      if (a1 == null) return 1
//      if (a2 == null) return -1
//      val res: Int = comp.get(i).compare(a1, a2)
//      if (res != 0) return res
//    }
//    return Integer.compare(o1.size, o2.size)
//  }

}