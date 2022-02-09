package com.github.maartyl.gdb.jxm

import com.github.maartyl.gdb.GPrimaryStrIndex
import com.github.maartyl.gdb.GRef
import com.github.maartyl.gdb.NodeBase

private val forbiddenRegex = Regex("[()*]")

internal class PrimaryStrIndexImpl<TN : NodeBase>(
  private val g: GDbImpl,
  override val name: String,
  val keyView: (TN) -> String,
) : GPrimaryStrIndex<TN> {

  private fun escapeKey(key: String): String {
    //probably way too expensive, but fine for now
    return key.replace(forbiddenRegex) {
      when (it.value[0]) {
        '(' -> "*L"
        ')' -> "*R"
        '*' -> "*A"
        else -> error("invalid escape match: ${it.value}")
      }
    }
  }

  @Suppress("UNCHECKED_CAST")
  override fun deriveRef(key: String): GRef<TN> {
    return g.internRef("$name(${escapeKey(key)})")
  }

  override fun primaryKeyOf(node: TN): String {
    return keyView(node)
  }
}