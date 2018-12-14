/*
 * Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between you or your company
 * and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
 * reproduction, modification, redistribution, sharing, lending or other exploitation
 * of all or any part of the contents of this software is strictly prohibited.
 */

package com.hortonworks.dataplane.profilers.tablestats.aggregators

import com.hortonworks.dataplane.profilers.tablestats.utils.StatsMetrics

class StatsAggregator extends Aggregator[StatsAccumulator, Number] with Serializable {
  override def initialElement = new StatsAccumulator()

  override def mergeValue(acc: StatsAccumulator, elem: Number) = {
    if(elem != null) acc.add(elem.doubleValue())
    else acc
  }

  override def mergeCombiners(acc1: StatsAccumulator, acc2: StatsAccumulator) = acc1.merge(acc2)

  override def result(acc: StatsAccumulator) = {
    new StatsMetrics(acc.result())
  }

  override def getState(acc: StatsAccumulator) = StatsState(acc)
}

case class StatsAccumulator(var min : Double = Double.MaxValue, var max : Double = Double.MinValue, var sum : Double = 0.0 , var squaredSum : Double = 0.0, var count: Int = 0) extends Serializable {

  def add(elem: Double) = {
    if (elem < min) min = elem
    if (elem > max) max = elem

    sum += elem
    count += 1
    squaredSum += elem * elem
    this
  }

  def merge(acc: StatsAccumulator) = {
    if (acc.min < min) min = acc.min
    if (acc.max > max) max = acc.max

    sum += acc.sum
    count += acc.count
    squaredSum += acc.squaredSum
    this
  }

  def result() = {
    val mean = if(count != 0) sum / count else 0
    val variance = if(count != 0 && count != 1)(squaredSum - count * mean * mean) / (count - 1) else 0
    val stdDev = Math.sqrt(variance)
    Stats(min, max, stdDev, mean, count)
  }

}

case class Stats(min: Double, max: Double, stdDev: Double, mean: Double, count: Double)
