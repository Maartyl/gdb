package com.github.maartyl.gdb

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.serialization.ContextualSerializer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable

//represents ID of a Node in Graph
// - must be "serializable"
// ACTUALLY: completely unused: GRef<*> is good enough, if not better.
// -- maybe remove completely?
interface GId

//ACTUALLY - UNUSABLE if class - if I want GRef to be able to point to Interfaces
// - can ONLY be an interface that all those interfaces would have to extend
typealias NodeBase = Any

//represents a TYPED ID of a Node with value T.
// T is just a HINT - can be unsafeCast to anything, and will still work, until someone tries to use derefed value
@Serializable(with = GRefCtxSerializer::class)
interface GRef<out T : NodeBase> : GId {
  //IF inside Snap: uses snap to deref
  //IF outside: starts new read-only qry
  suspend fun derefImpl(): NodeBase?

  //TODO: can this cause deadlock or so?
  // - ONLY if a QRY WAITS for a deref, made WITHOUT qry context - which is WRONG to do.
  // - so, yes, slightly unsafe, but... probably not for anything correct

  //TODO: there are multiple deref options
  // - ignoring snaps, check cache first - very fast
  // - check snap, use that - null/throw outside snap
  // - check snap, if outside: start read qry
  // different may be desired at different times, BUT: very annoying to need to pass in the Snap directly ...
  // - it made it impossible to implement the inline cast check deref ... - so, needed through ctx...
}

suspend inline fun <reified T : NodeBase> GRef<T>.deref() = derefImpl() as? T
suspend inline operator fun <reified T : NodeBase> GRef<T>.invoke(snap: GDbSnap) = snap.derefImpl(this) as? T

//TODO: idea GKey vs GPut
// - GKey : GRef and ONLY GKey can be used for PUT - to avoid putting accidentally wrong
// - or GPut ifce for "top" objs for storing in DB, and only those can be put in DB - for the same reason
//   - would replace :NodeBase in those api: as NodeBase will have to be an ifce, and GTop can always extend it.
//   - alt name: GObj or ... GNode ? (as in, the whole node)
// I have no RT mechanism to CHECK that a value is valid to be put into a Ref.

//to be implemented by ALL nodes to be saveable in DB
// BUT: only by FINAL types. (not interfaces, not intermediate) OR if Ref MEANT to be "Sealed union" then that.
// - This assures, that PUT is never done with something unexpected
// (must always cast ref to  GRef<T:GPut> to be sure - reminder
// - does NOT fully assure all perfect, but good enough...
// - each Ref should be associated with a
/** Represents an Object (Node) that can be PUT into the graph database.*/
interface GPut /*: NodeBase*/ {
  //TODO: scope impl  // user can create subscopes, and CLOSE them - that DELETES all nodes under that scope
  //      and transitively also closes all subscopes
  //      99% sure: Scopes should NOT be possible to restart: once over, is over.
  // val lifeScope : {dflt=Forever, Mem, SubScope(scope)}
  // - NOT GOOD:
  //   - Needs an arbitrary EDGE; mainly: to belong to multiple scopes
  //   also, there is no reason why arbitrary obj cannot be a scope - what matters are the connections
  //   so maybe: LifeScopeEdge{src:Scope=GRef<Any>, dst:GRef<Any>= to be deleted when src is deleted}
  //   BUT: this requires storing an extra OBJECT for each ... but is independent...
  // OR: have scopes, have single parent, BUT allow MULTI-SCOPE obj - a Subscope of multiples scopes...
  // - managing the extra edge-objects would be annoying and error prone...

  //mem scope problem: find on indexes must be implemented differently.
  // - if different map: wrong ORDER in iterating index
  // - but mem objs are FUTURE thing ...
}

//TODO:  "FK edges" == auto delete edge, if it references node that gets deleted
// - or rather! INDEXES! option to an index on GRef to:
// -- if deleted, delete me too ...

interface GIndex<TKey : Any, TN : NodeBase> {
  //name of this index
  val name: String

  fun find(snap: GDbSnap, key: TKey): Flow<GRef<TN>>
}

@Suppress("NOTHING_TO_INLINE")
inline fun <TN : NodeBase> GIndex<Unit, TN>.find(snap: GDbSnap) = find(snap, Unit)

interface GRangeIndex<TKey : Any, TN : NodeBase> : GIndex<TKey, TN> {

  //values in rslt can REPEAT if it returned multiple keys
  // ... hmm... is that an issue?
//  fun findRange(
//    snap: GDbSnap,
//    //null -> unconstrained
//    startKey: TKey?,
//    endKey: TKey?,
//    startInclusive: Boolean = true,
//    endInclusive: Boolean = true
//  ): Sequence<GRef<TN>>
}

//
//interface GUniqueIndex<TKey, TN : NodeBase> : GIndex<TKey, TN> {
//  //id obtained by fn(TN) ?
//  fun insertOrThrow(tx: GDbTx, node: TN): GRef<TN>
//  fun insertOrReplace(tx: GDbTx, node: TN): GRef<TN>
//  fun insertOrOld(tx: GDbTx, node: TN): GRef<TN>
//  fun find1(snap: GDbSnap, key: TKey): GRef<TN>?
//
//  //derives Ref, whether inserted or not
//  //TODO: is this possible? (if ref just stores key, then yes, otherwise ... ?)
//  //fun deriveRef(key: TKey): GRef<TN>
//}

//NAME: maybe IDENTITY index ? - as the key is all-time identity of the node ...
// is UniqueIndex, but skipping dealing with it for now
interface GPrimaryStrIndex<TN : NodeBase> : GIndex<String, TN> {
  //TODO: decide: should be TN:GPut ?

  //derives Ref, whether inserted or not
  fun deriveRef(key: String): GRef<TN>

  //TODO: maybe do not return if not in DB ?
  // - probably not important: nobody will be using FIND on primary index anyway
  override fun find(snap: GDbSnap, key: String): Flow<GRef<TN>> {
    return flowOf(deriveRef(key))
  }

  fun primaryKeyOf(node: TN): String
//
//  override fun find1(snap: GDbSnap, key: String): GRef<TN>? {
//    return deriveRef(key)
//  }
}

suspend fun <TN : GPut> GDbTx.put(idx: GPrimaryStrIndex<TN>, node: TN): GRef<TN> {
  //acts as insertOrReplace
  return idx.deriveRef(idx.primaryKeyOf(node)).also { it.put(node) }
}


@Suppress("UNCHECKED_CAST")
inline fun <reified TL : NodeBase, TR : Any> GDbBuilder.reverseIndexStr(
  name: String, noinline seri: (TR) -> String,
  crossinline view: (GRef<TL>, TL, MutableCollection<TR>) -> Unit,
): GRangeIndex<TR, TL> = reverseIndexRawStr<TR>(name, seri) { r, v, c ->
  (v as? TL)?.let { view(r as GRef<TL>, it, c) }
} as GRangeIndex<TR, TL>

@Suppress("UNCHECKED_CAST")
inline fun <reified TL : NodeBase, TR : Any> GDbBuilder.reverseIndexLong(
  name: String, noinline seri: (TR) -> Long,
  crossinline view: (GRef<TL>, TL, MutableCollection<TR>) -> Unit,
): GRangeIndex<TR, TL> = reverseIndexRawLong<TR>(name, seri) { r, v, c ->
  (v as? TL)?.let { view(r as GRef<TL>, it, c) }
} as GRangeIndex<TR, TL>

@Suppress("UNCHECKED_CAST")
inline fun <reified TL : NodeBase, TR : NodeBase> GDbBuilder.reverseReverseIndex(
  name: String,
  crossinline view: (GRef<TL>, TL, MutableCollection<GRef<TR>>) -> Unit,
): GIndex<GRef<TR>, TL> = reverseIndexRawGRef<TR>(name) { r, v, c ->
  (v as? TL)?.let { view(r as GRef<TL>, it, c) }
} as GIndex<GRef<TR>, TL>


//@Suppress("UNCHECKED_CAST")
//inline fun <TG : Any, reified TN : NodeBase> GDbBuilder.groupIndex(
//  name: String, seri: KSerializer<TG>,
//  crossinline view: (GRef<TN>, TN) -> TG?,
//): GIndex<TG, TN> =
//  groupIndexRaw<TG>(name, seri) { r, v ->
//    (v as? TN)?.let { view(r as GRef<TN>, it) }
//  } as GIndex<TG, TN>


interface GDbBuilder {
  suspend fun build(): GDb


  //name must be unique among all indexes in GDb
  // view defines relationship; view is run for ALL nodes in db, after each CHANGE of that node (including add)
  // index.find(TR) returns all TL that returned the TR
//  fun <TR> reverseIndexRaw(
//    name: String,
//    seri: KSerializer<TR>,
//    //ret null == this ref is never indexed (not just empty in this case)
//    //if returns null, must not add to coll
//    view: (GRef<*>, NodeBase, MutableCollection<TR>) -> Unit?,
//  ): GIndex<TR, *>

  //TODO: change these (names) to ENSURE index, and also do somehow ensure it fully reflects latest state...?

  // REVERSE because: creates index that is reverse (/inverse?) of the VIEW FN
  // (technically, it returns the refs, not the Node obj, so not 100% inverse, but essentially)
  // (also, it returns for ANY in rslt-set ALL who returned it... so even less reverse...)
  // (... idk. I like the name, but if there is better, I will change it)
  // ! in a GRAPH - view provides FORWARD edges -- REVERSE edges are computed by the index
  // view must be a PURE function (as in, always "returns" the same set, purely derived by Node)

  fun <TR : Any> reverseIndexRawStr(
    name: String,
    seri: (TR) -> String,
    //ret null == this ref is never indexed (not just empty in this case)
    //if returns null, must not add to coll
    view: (GRef<*>, NodeBase, MutableCollection<TR>) -> Unit?,
  ): GRangeIndex<TR, *>

  fun <TR : Any> reverseIndexRawLong(
    name: String,
    seri: (TR) -> Long,
    //ret null == this ref is never indexed (not just empty in this case)
    //if returns null, must not add to coll
    view: (GRef<*>, NodeBase, MutableCollection<TR>) -> Unit?,
  ): GRangeIndex<TR, *>

  fun <TR : NodeBase> reverseIndexRawGRef(
    name: String,
    //ret null == this ref is never indexed (not just empty in this case)
    //if returns null, must not add to coll
    view: (GRef<*>, NodeBase, MutableCollection<GRef<TR>>) -> Unit?,
  ): GIndex<GRef<TR>, *>


  //for nodes with externally defined PK
  //TG needs well defined equality
  // if returns null, will NOT be part of any group
  // TId probably must be String or something... - maybe change to only String nad Int and ..? impl... ?
  // - OR: if STR - can be used directly; if weird: is a special table...
  // will need more args, including reified type, etc.
//  fun <TId : Any, TN : NodeBase> uniqueIndex(
//    name: String,
//    seri: KSerializer<TId>,
//    id: (TN) -> TId,
//  ): GIndex<TId, TN>


  //MOST IMPORTANT - allows INSERTING ids that are not auto-generated
  fun <TN : NodeBase> primaryIndex(
    name: String,
    id: (TN) -> String,
  ): GPrimaryStrIndex<TN>


  //TODO: maybe secondary fn: FILTER (synchronous) - if returns false: trigger will not be enqueued
  // + inline extension method, that puts TYPE check in filter
  //btw.: if trigger makes changes, those can in turn cause MORE triggers to be enqueued
  // - potentially infinite??
  //all triggers run queued at the END of a TX (but if update fails, TX fails)
  //TOUP: pass special GDbSnapTig .mutate{} - so many triggers can be started in parallel
  fun addTriggerRaw(
    //run for each TN that changes
    trigger: suspend GDbTx.(GRef<NodeBase>) -> Unit,
  )
}

//runs build WITHOUT requiring SUSPENDING fn
// - returns a DELEGATE GDb, where each call first awaits the built
// - this is useful, if you need to call build() in a CONTRUCTOR of a wrapper class
fun GDbBuilder.buildBg(scope: CoroutineScope): GDb {
  val built = scope.async { build() }
  return object : GDb {
    override suspend fun <T> read(block: suspend GDbSnap.() -> T): T {
      return built.await().read(block)
    }

    override suspend fun <T> mutate(block: suspend GDbTx.() -> T): T {
      return built.await().mutate(block)
    }

    override fun <T : NodeBase> subscription(ref: GRef<T>): Flow<T?> {
      return flow { emitAll(built.await().subscription(ref)) }
    }

    override fun <T> subscription(block: suspend GDbSnap.() -> T): Flow<T> {
      return flow { emitAll(built.await().subscription(block)) }
    }
  }

}

//represents a GRAPH of nodes
interface GDb {

//  //UNNECESARY: probably quite pointless ... normal trigger is better
//  //all updates run queued at the END of a TX (but if update fails, TX fails)
//  fun <T1 : NodeBase, T2 : NodeBase> addTriggerUpdate(
//    //run for each T1 that changes
//    // - returns all refs that should be updated in response
//    trigger: GDbSnap.(GRef<T1>, T1) -> Iterable<GRef<T2>>,
//    //updates the T2
//    //TOUP: is it ok to pass in TX ? ... I think it shouldnt update anything else...
//    update: GDbSnap.(GRef<T1>, T1, GRef<T2>, T2?) -> T2?
//  ) = addTrigger<T1> { r1 ->
//    deref(r1)?.let { trigger(r1, it).forEach { r2 -> put(r2, this.update(r1, it, r2, deref(r2))) } }
//  }


  //GDbSnap can only be used inside the block
  suspend fun <T> read(block: suspend GDbSnap.() -> T): T

  //GDbTx can only be used inside the block
  suspend fun <T> mutate(block: suspend GDbTx.() -> T): T

  //emits on every change + first time
  //emits null if not in DB
  fun <T : NodeBase> subscription(ref: GRef<T>): Flow<T?>

  //tracks all deferred refs from last invoke of block, and if any of those change: reruns
  fun <T> subscription(block: suspend GDbSnap.() -> T): Flow<T>
}

//allows for consistent reads from a snapshot of graph
interface GDbSnap {

  //null if not in graph
  suspend fun <T : NodeBase> derefImpl(gRef: GRef<T>): NodeBase?

  //final
  //cannot be defined as extension, since it's using it
//  @JvmName("derefExt")
//  fun <T : NodeBase> GRef<T>.deref(): T? = deref(this)
}

inline val <T : GDbSnap> T.snap get() = this

// NOT THREAD SAFE - It is your responsibility to not access it concurrently WITHIN one transaction.
// - separate transactions are independent - that is fine
// - must not be used after mutate{} completes
interface GDbTx : GDbSnap {
  //creates a node with NEW ID
  suspend fun <T : GPut> insertNew(node: T): GRef<T>

  //MAYBE: better name
  //inserts or updates or removes
  // UNSAFE VARIANCE - theoretically allows to put ANY value to all refs cast to arbitrary T
  // - put MUST be done CAREFULLY and correctly, as the rest of app expects
  suspend fun <T : GPut> GRef<T>.put(node: T?)
}

suspend inline fun <T : GPut> GDbTx.put(ref: GRef<T>, node: T?) = ref.put(node)

@OptIn(ExperimentalSerializationApi::class)
object GRefCtxSerializer : KSerializer<GRef<*>> by ContextualSerializer(GRef::class, null, arrayOf())

//@OptIn(ExperimentalSerializationApi::class)
//object GRefCtxSerializer2 : KSerializer<GRef<*>> {
//
//  object PrimitiveSerialDescriptor : SerialDescriptor {
//    override val serialName: String = "GRefCtx"
//    override val kind = SerialKind.CONTEXTUAL
//    override val elementsCount: Int get() = 0
//    override fun getElementName(index: Int): String = error()
//    override fun getElementIndex(name: String): Int = error()
//    override fun isElementOptional(index: Int): Boolean = error()
//    override fun getElementDescriptor(index: Int): SerialDescriptor = error()
//    override fun getElementAnnotations(index: Int): List<Annotation> = error()
//    override fun toString(): String = "ContextualDescriptor($serialName)"
//    private fun error(): Nothing = throw IllegalStateException("Primitive descriptor does not have elements")
//  }
//
//  override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor
//
//  private fun serializer(serializersModule: SerializersModule): KSerializer<GRef<*>> =
//    serializersModule.getContextual(GRef::class) ?: error("contextual serializer for GRef not provided")
//
//  override fun serialize(encoder: Encoder, value: GRef<*>) {
//    encoder.encodeSerializableValue(serializer(encoder.serializersModule), value)
//  }
//
//  override fun deserialize(decoder: Decoder): GRef<*> {
//    return decoder.decodeSerializableValue(serializer(decoder.serializersModule))
//  }
//}