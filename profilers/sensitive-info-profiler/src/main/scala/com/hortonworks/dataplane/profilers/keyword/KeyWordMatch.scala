/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.dataplane.profilers.keyword


//Credits - https://github.com/yizhiru/scala-implementation-of-aho-corasick

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class KeyWordMatch {

  // initialization
  val trie: ArrayBuffer[Node] = ArrayBuffer(
    Node(state = 0,
      value = 0.asInstanceOf[Byte],
      goto = mutable.Map[Byte, Int](),
      failure = 0,
      output = mutable.Set[(String, String)]())
  )

  /**
    * Goto a state according to byte value.
    *
    * @param node current node.
    * @param byte byte value.
    * @return destination node.
    */
  private def gotoState(node: Node, byte: Byte): Int = {
    node.goto.getOrElse(byte, -1)
  }

  /**
    * Add key word.
    *
    * @param property word property
    * @param word     key word
    */
  def addWord(property: String, word: String): Unit = {
    var state = 0
    word.getBytes().foreach { t =>
      gotoState(trie(state), t) match {
        case -1 => val next = trie.size
          trie(state).goto += (t -> next)
          trie += Node(next, t, mutable.Map[Byte, Int](), 0, mutable.Set[(String, String)]())
          state = next
        case _ => state = gotoState(trie(state), t)
      }
    }
    trie(state).output += ((word, property))
  }

  /**
    * Add key words.
    *
    * @param property word property.
    * @param words    key words.
    */
  def addWords(property: String, words: List[String]): Unit = {
    words.foreach {
      word => addWord(property, word)
    }
  }

  /**
    * Set failure function.
    */
  def setFailTransitions(): Unit = {
    val queue = mutable.Queue[Int]()
    // set failure for node whose depth=1
    trie(0).goto.foreach { case (a, s) =>
      queue += s
      trie(s).failure = 0
    }
    while (queue.nonEmpty) {
      val r = queue.dequeue()
      trie(r).goto.foreach { case (a, s) =>
        queue += s
        var state = trie(r).failure
        while (gotoState(trie(state), a) == -1 && state != 0)
          state = trie(state).failure
        val goto_a: Int = if (state == 0 && gotoState(trie(state), a) == -1) {
          0
        } else {
          trie(state).goto.getOrElse(a, 0)
        }
        trie(s).failure = goto_a
        trie(s).output ++= trie(trie(s).failure).output
      }
    }
  }

  /**
    * Search words.
    *
    * @param str string.
    * @return matched key words.
    */
  def search(str: String): mutable.Set[(String, String)] = {
    val resultSet = mutable.Set[(String, String)]()
    var node = trie(0)
    str.getBytes.foreach { a =>
      while (gotoState(node, a) == -1 && node.state != 0)
        node = trie(node.failure)
      node = node.state == 0 && gotoState(node, a) == -1 match {
        case true => trie(0)
        case _ => trie(gotoState(node, a))
      }
      if (node.output.nonEmpty)
        resultSet ++= node.output
    }
    resultSet
  }
}

object KeyWordMatch {

  import org.json4s.DefaultReaders._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def fromJson(keywordStore : String) = {
    val matcher = new KeyWordMatch()
    parse(keywordStore).asInstanceOf[JArray].arr.map(entry => {
      val label = (entry \ "label").as[String]
      val tokens = (entry \ "tokens").asInstanceOf[JArray].arr.map(_.as[String])
      matcher.addWords(label, tokens)
    })
    matcher.setFailTransitions()
    matcher
  }
}