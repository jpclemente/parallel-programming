package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @scala.annotation.tailrec
    def loop(chars: List[Char], acc: Int): Boolean = chars match {
      case Nil => acc == 0
      case h::t => h match {
        case '(' => loop(t, acc + 1)
        case ')' => if (acc <= 0) false else loop(t, acc -1)
        case _ => loop(t, acc)
      }
    }
    loop(chars.toList, 0)
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, closingNum: Int, openingNum: Int): (Int, Int) = {
      var closingsOverflow = 0
      var openings = 0

      var i = idx
      while (i < until) {
        chars(i) match {
          case '(' => openings = openings + 1
          case ')' =>
            if (openings > 0) openings = openings - 1
            else closingsOverflow = closingsOverflow + 1
          case _ =>
        }
        i = i + 1
      }
      (closingsOverflow, openings)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      }
      else {
        val mid = (until + from)/2
        val ((c1, o1), (c2, o2)) = parallel(reduce(from, mid), reduce(mid, until))
        if (o1 > c2) (c1, o1 - c2 + o2)
        else (c1 + c2 - o1, o2)
      }
    }
    reduce(0, chars.length) == (0,0)
  }
}
