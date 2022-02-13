package com.github.maartyl.gdb.jxm

import com.github.maartyl.gdb.*
import com.google.common.collect.MapMaker
import kotlinx.collections.immutable.persistentSetOf
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.*
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.protobuf.ProtoBuf
import org.mapdb.DB
import org.mapdb.Serializer
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext


//nodes must be Serializable
fun <T : NodeBase> gdbJxmOpen(
  //assumes NOT IO Dispatcher - is for maintanance around
  //must have JOB - Once Completes - CLOSES rw
  scope: CoroutineScope,
  nodeSeri: KSerializer<T>,
  //ro: DB,
  //ReadWrite backing DB
  rw: DB,
  //for running queries
  ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
): GDbBuilder {
  // TODO ... hmm, the Transactions are quite confusing ...
  //  - if whole DB is a single transaction... do I need another for read only stuff?
  //  - would they share mem, or completely separate, only same file??
  // or maybe use my own, single-thread executor ?
  // - but MapDB claims to be thread-safe, so ... it is, but not transactions???
  val j = Job(scope.coroutineContext.job)
  val impl = GDbImpl(coroutineContext = scope.coroutineContext + j, rw, nodeSeri, ioDispatcher)
  scope.launch {
    //probably must wait until completed, not just awaitCancellation()
    //rw.use { awaitCancellation() }
    rw.use { j.join() }
  }
  return impl
}

@JvmInline
internal value class Valid<T>(val o: Any?) {
  @Suppress("UNCHECKED_CAST")
  inline fun validOr(block: () -> T) = if (isValid()) o as T else block()

  fun isValid() = o !== INVALIDATED

  companion object {
    fun <T> invalidated() = Valid<T>(INVALIDATED)
    fun <T> valid(t: T) = Valid<T>(t)
  }

  private object INVALIDATED {
    override fun toString() = "INVALIDATED"
  }
}

//@Suppress("SERIALIZER_TYPE_INCOMPATIBLE")
//@OptIn(ExperimentalSerializationApi::class)
//OOH! nice! This is not needed at all. - all is solved by polymorphic thingy - great
//@Serializable(with = RefCtxSerializer::class)
internal class Ref<T : NodeBase>(
  val g: GDbImpl,
  //format: type(args)
  //- for now, args interpretation depends on type, BUT: cannot contain parens EXCEPT as grouping of subkeys
  // - comma should be used to separate multiple args, but only interpreted by the specific type
  // - TODO: replace copmma with * - continuous block of ASCII; also even less used probably
  // - for encoded escaped strs use:  ( ) * -> *L *R *S respectively
  // -- unless it also needs multiple args, in which case... idk: a 4th special char just for escaping?
  // -- how about ASCII-ESC for escaping? that's a good idea!
  // -- and maybe even for parens? who's gonna look at it?! - it's just for internal stuff
  //    and almost all things can handle arbitrary unicode...
  //    - and escaping will almost never be needed...
  // ... although: ids that are easy for user to use are nice too,
  //    especially if I want some text UI, clipboard copying, ...
  //i.e. it is possible:  edge(_g(a3),idx(hello))

  //TODO! NEW IDEA! - better!?
  // - will NEVER be parsed, but can be safely derived - always the same
  // - format:
  //   #(<sha1>)           //keys TOO LONG (longer than sha1+3)
  //     hmm... this saves space, but prevents knowing node type from ID ... would parsing ever be worth it?
  //   %(<gen-str>)        //generated keys
  //   indexName(pkValue)     // keys from "primary" indexes  or idIndex or... - no way to iterate...
  //example edge:  +(type*%(left)*%(right))
  // ... WAIT: cannot edge type be just part of name???
  //  +edgeType(%(left)*%(right))
  // SEPARATOR? if multi-args can only be other ids: separator is not needed: parens are enough
  val id: String,

  //IDEA: also store a COLLECTION to which this ref belongs - would act as namespace (has name too)
  // - still: GRef is ref to ANY coll, can mix in a list, user does not care
  // - DIFFERENCE: allows creating each cool with OWN SERIALIZER and ONLY put through it
  //   - hence: writes are safe (unlike now) and no need to parametrize whole GDb - only the coll
  //   - the coll would be obtained similar to an index
  //   - ref.put(node) STILL possible, since Ref knows it's coll ..... dammit. UNTYPED ...
  //   - so, put would be more annoying ...
  //   - MORE IMPORTANTLY: how would I get the coll when deserializing? from type in id?
  // well, not gonna do it for now, but possibility
) : GRef<T>, suspend GDbSnap.() -> NodeBase? {

  override fun hashCode(): Int {
    return id.hashCode()
  }

  override fun equals(other: Any?): Boolean {
    val o = other as? Ref<*> ?: return false
    return id == o.id
  }

  override fun toString(): String {
    return "Ref($id)"
  }

  //if invalidated: reading this node reads from db

  internal var cachedNode: Valid<T?> = Valid.invalidated()

  //invoked when ref node gets changed value
  fun invalidate() {
    cachedNode = Valid.invalidated()
  }

  fun cacheLatest(node: T?) {
    cachedNode = Valid.valid(node)
  }

  override suspend fun derefImpl(): NodeBase? = when (val snap = coroutineContext[KeySnapTx]) {
    null -> {
      //TODO: mark this as FAST qry
      g.read(this)
    }
    else -> {
      snap.derefImpl(this)
    }
  }

  override suspend fun invoke(snap: GDbSnap): NodeBase? = snap.derefImpl(this)
}

internal val <T : NodeBase> GRef<T>.asRef get() = this as? Ref<T> ?: error("bad Ref $this")

internal interface ChangeObserver {

  fun txStart(tx: TxImpl) {}
  fun txPreCommit(tx: TxImpl) {}

  //actually, nothing may be changed anymore...
  fun txPostCommit(tx: SnapImpl) {}
  fun txPreRollback(tx: TxImpl) {}
  fun txPostRollback(tx: SnapImpl) {}

  fun <TN : NodeBase> onNodeChanged(tx: TxImpl, ref: Ref<TN>, old: TN?, new: TN?)

  //TODO: onIndexChanged(name, key)  --  (for subscribe)

  //fun nodesChanged(tx: TxImpl, ns: List<Ref<*>>)
}

internal object C {


  const val PX_IDX = "index!"
  const val NODES = "graph!nodes"
  const val NODE_ID_GEN = "graph!nodeIdGen"

  //  val refDescriptor = PrimitiveSerialDescriptor(
//    "com.github.maartyl.jxm.RefAsStringSerializer", PrimitiveKind.STRING
//  )
  val refDescriptor = PrimitiveSerialDescriptor(
    "jxm.RS", PrimitiveKind.STRING
  )
}

//@OptIn(ExperimentalSerializationApi::class)
//object RefCtxSerializer : KSerializer<GRef<*>> {
//
//  object PrimitiveSerialDescriptor : SerialDescriptor {
//    override val serialName: String = "com.github.maartyl.jxm.RefCtxSerializer"
//    override val kind = SerialKind.CONTEXTUAL
//    override val elementsCount: Int get() = 0
//    override fun getElementName(index: Int): String = error()
//    override fun getElementIndex(name: String): Int = error()
//    override fun isElementOptional(index: Int): Boolean = error()
//    override fun getElementDescriptor(index: Int): SerialDescriptor = error()
//    override fun getElementAnnotations(index: Int): List<Annotation> = error()
//    override fun toString(): String = "PrimitiveDescriptor($serialName)"
//    private fun error(): Nothing = throw IllegalStateException("Primitive descriptor does not have elements")
//  }
//
//  private fun serializer(serializersModule: SerializersModule): KSerializer<GRef<*>> =
//    serializersModule.getContextual(GRef::class) ?: error("contextual serializer for Ref not provided")
//
//  override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor
//
//  override fun serialize(encoder: Encoder, value: GRef<*>) {
//    encoder.encodeSerializableValue(serializer(encoder.serializersModule), value)
//  }
//
//  override fun deserialize(decoder: Decoder): GRef<*> {
//    return decoder.decodeSerializableValue(serializer(decoder.serializersModule))
//  }
//}


//private class InterningRefSerializer(private val loadRef: (String) -> Ref<*>) : KSerializer<Ref<*>> {
//  override val descriptor: SerialDescriptor get() = C.refDescriptor
//  override fun serialize(encoder: Encoder, value: Ref<*>) = encoder.encodeString(value.id)
//  override fun deserialize(decoder: Decoder): Ref<*> = loadRef(decoder.decodeString())
//}

private class InterningGRefSerializer(private val loadRef: (String) -> Ref<*>) : KSerializer<GRef<*>> {
  override val descriptor: SerialDescriptor get() = C.refDescriptor
  override fun serialize(encoder: Encoder, value: GRef<*>) = encoder.encodeString(value.asRef.id)
  override fun deserialize(decoder: Decoder): GRef<*> = loadRef(decoder.decodeString())
}

internal class GDbImpl(
  override val coroutineContext: CoroutineContext,
  //ro: DB,
  val rw: DB,
  private val nodeSeri: KSerializer<*>,
  val ioDispatcher: CoroutineDispatcher,
) : GDb, GDbBuilder, CoroutineScope {

  //TOUP: save Thread reference and ONLY allow build calls from that thread
  private var isBuilding = true
  private var isBuilt = false
  override suspend fun build(): GDb {
    checkBuilding()
    isBuilding = false
    rw.commit()
    //TODO: run mem-reindexing
    isBuilt = true
    return this
  }

  private fun checkBuilding() {
    if (!isBuilding) error("GDb already built.")
  }

  private fun checkBuilt() {
    if (!isBuilt) error("GDb not built yet.")
  }

  //val weakRefs = mutableMapOf<Long, WeakReference<Ref<*>>>()
  //refs hold ref to latest Node obj - if nobody holds ref to it: don't keep it in memory
  private val weakRefs = MapMaker().weakValues().makeMap<String, Ref<*>>()

  private fun internRefUntyped(id: String): Ref<*> {
    return internRef<NodeBase>(id)
  }

  @Suppress("UNCHECKED_CAST")
  fun <T : NodeBase> internRef(id: String): Ref<T> {
    return weakRefs.getOrPut(id) { Ref<T>(this, id) } as Ref<T>
  }

  private val grefSeri = InterningGRefSerializer(this::internRefUntyped)

  @OptIn(ExperimentalSerializationApi::class)
  val proto = ProtoBuf {
    serializersModule = SerializersModule {
      contextual(grefSeri)

      //argh, still not great: this saves name of the serializer with each REF because it's polymorphic
      // - even though it will never be anything else ...
      // dammit, still not perfect
      //polymorphic(GRef::class, Ref::class, grefSeri)
    }
  }

  @Suppress("UNCHECKED_CAST")
  private val nodes = rw.hashMap(C.NODES, Serializer.STRING, Serializer.BYTE_ARRAY).createOrOpen()

  @Suppress("UNCHECKED_CAST")
  suspend fun <T : NodeBase> nodesGetAndCache(ref: Ref<T>): T? {
    return withContext(ioDispatcher) { nodes[ref.id] }?.let {
      @OptIn(ExperimentalSerializationApi::class) proto.decodeFromByteArray(nodeSeri, it) as T
    }.also { ref.cacheLatest(it) }
  }

  //PUT and GET OLD
  @Suppress("UNCHECKED_CAST")
  suspend fun <T : NodeBase> nodesPutSwap(ref: Ref<T>, node: T?): T? {
    val oldSerNode = if (node == null) {
      withContext(ioDispatcher) {
        nodes.remove(ref.id)
      }
    } else {
      val nodeBase: NodeBase = node

      @OptIn(ExperimentalSerializationApi::class) val serNode =
        proto.encodeToByteArray(nodeSeri as KSerializer<NodeBase>, nodeBase)

      withContext(ioDispatcher) {
        //there is no way to NOT deserialize old value (when already in cachedNode)
        nodes.put(ref.id, serNode)
      }
    }
    //assumes called BEFORE ref updated -- cachedNode still valid
    return if (oldSerNode == null) null else @OptIn(ExperimentalSerializationApi::class) ref.cachedNode.validOr {
      proto.decodeFromByteArray(
        nodeSeri,
        oldSerNode
      ) as T
    }

  }

  val nodeIdGen = rw.atomicLong(C.NODE_ID_GEN).createOrOpen()

  val chngo = ChangeDispatcher(this)

  //for checking while building, so none overlap
  val tmpIndexNames = mutableSetOf<String>()

  override fun <TR : Any> reverseIndexRawStr(
    name: String, seri: (TR) -> String, view: (GRef<*>, NodeBase, MutableCollection<TR>) -> Unit?
  ): GRangeIndex<TR, *> {
    checkBuilding()
    if (name in tmpIndexNames) error("Repeated index name: $name")
    tmpIndexNames.add(name)
    return multiIndexStr<TR, NodeBase>(this, name, view, seri).also {
      chngo.register(it)
    }
  }

  override fun <TR : Any> reverseIndexRawLong(
    name: String, seri: (TR) -> Long, view: (GRef<*>, NodeBase, MutableCollection<TR>) -> Unit?
  ): GRangeIndex<TR, *> {
    checkBuilding()
    if (name in tmpIndexNames) error("Repeated index name: $name")
    tmpIndexNames.add(name)
    return multiIndexLong<TR, NodeBase>(this, name, view, seri).also {
      chngo.register(it)
    }
  }

  override fun <TR : NodeBase> reverseIndexRawGRef(
    name: String, view: (GRef<*>, NodeBase, MutableCollection<GRef<TR>>) -> Unit?
  ): GIndex<GRef<TR>, *> = run {
    checkBuilding()
    if (name in tmpIndexNames) error("Repeated index name: $name")
    tmpIndexNames.add(name)
    reverseIndexRawStr(name, { it.asRef.id }, view)
  }

  override fun <TN : NodeBase> primaryIndex(name: String, id: (TN) -> String): GPrimaryStrIndex<TN> {
    checkBuilding()
    if (name in tmpIndexNames) error("Repeated index name: $name")
    tmpIndexNames.add(name)
    return PrimaryStrIndexImpl(this, name, id)
  }

  override fun addTriggerRaw(trigger: suspend GDbTx.(GRef<NodeBase>) -> Unit) {
    TODO("Not yet implemented")
  }

  //TOUP: maybe pass in Dispatchers.Unconstrained ? - it will only ever do cheap synchronization stuff
  private val executor: RwExecutor = RwExecutor(this)

  private suspend fun <T> execEnqueue(readOnly: Boolean, block: suspend CoroutineScope.() -> T): T {
    return coroutineScope {
      //I need exceptions to be propagated only here + correct context...
      val work = async(ioDispatcher, start = CoroutineStart.LAZY, block)

      val h = this@GDbImpl.coroutineContext.job.invokeOnCompletion {
        work.cancel(CancellationException("GDb cancelled and closed", it))
      }

      executor.enqueue(object : RwSlot {
        override val readOnly = readOnly
        override suspend fun invoke(mustIgnore: CoroutineScope) {
          work.join()
          h.dispose()
        }
      })

      //SADLY!! this STARTS it - need another signal to start the block
      //work.await()
      work
      //AHA! - done like this: the coroutineScope cannot return until all children completed
      // - so work must run first, even though it was already returned
      // - the await cannot trigger start of work
    }.await()
  }

  override suspend fun <T> read(block: suspend GDbSnap.() -> T): T = execEnqueue(readOnly = true) {
    val snap = SnapImplBlock(this@GDbImpl, this, null, block)
    withContext(snap, snap)
  }

  override suspend fun <T> mutate(block: suspend GDbTx.() -> T): T = execEnqueue(readOnly = false) {
    val tx = TxImplBlock(this@GDbImpl, this, seen = null, block)
    withContext(tx, tx)
  }

  //cannot be SharedFlow - I could have a MAP of them, and un/register them as collectorsCount changes, but not worth it
  //  - after all: this is cheap, and it's not possible for the other subscription() anyway
  override fun <T : NodeBase> subscription(ref: GRef<T>): Flow<T?> {
    //TOUP: cheaper implementation - direct ChangeListener - pass new value
    // - WAIT: cannot if TX !! - still must enqueue itself for end of TX, but still faster
    return channelFlow {
      val co = SubscriptionRef(this@GDbImpl, this, ref.asRef)
      chngo.register(co)

      awaitClose {
        chngo.unregister(co)
      }
    }.conflate()
    //.buffer(1, onBufferOverflow = BufferOverflow.DROP_OLDEST)
  }

  //cannot be SharedFlow - each block needs own, and I will "never" get the same twice anyway
  // ALSO: SharedFlow does not forward EXCEPTIONS; and needs scope and ....
  override fun <T> subscription(block: suspend GDbSnap.() -> T): Flow<T> {

    return flow {
      val obs = object { //TODO: an impl of CO
        val changeSeen = MutableStateFlow(false)

        //starts with none - fine, as it will run anyway
        val toLookOutFor: MutableSet<Ref<*>> = mutableSetOf()

        //TODO: also seen INDEX lookups
        // - they are NOT necessarily covered by ref changes...
        // (for example: new node got added to the result set but never deref-ed in the subscription, etc.)
      }
      try {
// - SO long as I RUN - no TX can run - so: gotta register BEFORE I finish
        while (true) {

          //TODO: A!!! FFS ... what if change to INDEX ?? I would not be notified...
          // - all "root" things need to be saved in "seen" - including "checked index for key X"
          // - without this, could now be returning a different set, and I would not notice

          val toEmit = execEnqueue(readOnly = true) {
            obs.changeSeen.value = false
            obs.toLookOutFor.clear()
            val snap = SnapImplBlock(this@GDbImpl, this, obs.toLookOutFor, block)
            withContext(snap, snap)
          }
          emit(toEmit)

          obs.changeSeen.first { it } //suspend until something seen to have changed
        }
      } finally {
        //TODO: unregister obs
      }


    }
  }

}

internal class ChangeDispatcher(g: GDbImpl) : ChangeObserver {
  //TODO: maybe take snapshot at the start of Tx/Snap ?
  private var handlers = persistentSetOf<ChangeObserver>()

  //TODO: is it OK in the middle of a "tx" ?
  // - probably not: handlers may depend on seeing the whole Tx lifecycle...
  fun register(obs: ChangeObserver) = synchronized(handlers) {
    handlers = handlers.add(obs)
  }

  //TODO: is it OK in the middle of a "tx" ?
  fun unregister(obs: ChangeObserver) = synchronized(handlers) {
    handlers = handlers.remove(obs)
  }


  override fun <TN : NodeBase> onNodeChanged(tx: TxImpl, ref: Ref<TN>, old: TN?, new: TN?) {
    handlers.forEach { it.onNodeChanged(tx, ref, old, new) }
  }

  override fun txStart(tx: TxImpl) {
    handlers.forEach { it.txStart(tx) }
  }

  override fun txPreCommit(tx: TxImpl) {
    handlers.forEach { it.txPreCommit(tx) }
  }

  override fun txPostCommit(tx: SnapImpl) {
    handlers.forEach { it.txPostCommit(tx) }
  }

  override fun txPreRollback(tx: TxImpl) {
    handlers.forEach { it.txPreRollback(tx) }
  }

  override fun txPostRollback(tx: SnapImpl) {
    handlers.forEach { it.txPostRollback(tx) }
  }

}