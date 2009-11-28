import scala.collection.mutable.{HashMap, ArrayBuffer}

class NamedColumn(val table: Node) extends Node

object NamingContext {
  private val tnames = new HashMap[Node, String]
  private var nextTid = 1

  def nameFor(t: Node) = tnames.get(t) match {
    case Some(n) => n
    case None =>
      val n = "t" + nextTid
      nextTid += 1
      tnames.put(t, n)
      n
  }
}

trait Node {
  def nodeDelegate: Node = this
  def isNamedTable = false
}

trait Segment { def appendTo(res: StringBuilder): Unit }

class StringSegment extends Segment {
  val sb = new StringBuilder(32)
  def appendTo(res: StringBuilder): Unit = res append sb
}

final class SQLBuilder extends Segment {
  private val segments = new ArrayBuffer[Segment]
  private var currentStringSegment: StringSegment = null

  private def ss = {
    if(currentStringSegment eq null) {
      if(segments.isEmpty || segments.last.isInstanceOf[SQLBuilder]) {
        currentStringSegment = new StringSegment
        segments += currentStringSegment
      }
      else currentStringSegment = segments.last.asInstanceOf[StringSegment]
    }
    currentStringSegment
  }

  def +=(s: String) = { ss.sb append s; this }

  def createSlot = {
    val s = new SQLBuilder
    segments += s
    currentStringSegment = null
    s
  }

  def appendTo(res: StringBuilder): Unit = for(s <- segments) s.appendTo(res)
}

sealed trait TableBase extends Node with Cloneable { self =>
  override def isNamedTable = true
  def mapOp(f: Node => Node): this.type = {
    val t = clone
    t._op = f(this)
    t
  }
  private var _op: Node = _
  final def op: Node = _op
  override def clone(): this.type = super.clone.asInstanceOf[this.type]
  override def nodeDelegate: Node = if(op eq null) this else op.nodeDelegate
}

class Table extends TableBase { def id = new NamedColumn(nodeDelegate) }

final case class Alias(child: Node) extends Node { override def isNamedTable = true }

final class Join[+T1 <: TableBase, +T2 <: TableBase](_left: T1, _right: T2, val on: Node) extends TableBase {
  def right = _right.mapOp(n => JoinPart(n.nodeDelegate, nodeDelegate))
}

final case class JoinPart(left: Node, right: Node) extends Node

class BasicQueryBuilder(qval: Node) {

  protected val localTables = new HashMap[String, Node]
  protected var fromSlot: SQLBuilder = _

  protected def localTableName(n: Node) = n match {
    case JoinPart(table, from) =>
      localTables(NamingContext.nameFor(from)) = from
      NamingContext.nameFor(table)
    case _ =>
      val name = NamingContext.nameFor(n)
      localTables(name) = n
      name
  }

  final def buildSelect: String = {
    val b = new SQLBuilder
    expr(qval.nodeDelegate, b)
    fromSlot = b.createSlot
    for((name, t) <- new HashMap ++= localTables) { //-- used to iterate over mutable collection here!
      fromSlot += "{from "
      t match {
        case Alias(base: Table) => fromSlot += "TAB " += name
        case base: Table => fromSlot += "TAB " += name
        case j: Join[_,_] => fromSlot += "joinOn "; expr(j.on, fromSlot)
      }
      fromSlot += "}"
    }
    val sb = new StringBuilder
    b.appendTo(sb)
    sb.toString
  }

  protected def expr(c: Node, b: SQLBuilder): Unit = c match {
    case n: NamedColumn => { b += localTableName(n.table) += ".col" }
  }
}

object JoinTest extends Application {

  val t1 = new Table
  val t2 = new Table

  val j = new Join(t1, t2, t2.id.nodeDelegate)
  val q2 = j.right.id

  val s = new BasicQueryBuilder(q2).buildSelect
  println(s)
}
