/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.internal.Logging

/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the speed of ingestion of elements into Spark Streaming. A PID controller works
 * by calculating an '''error''' between a measured output and a desired value. In the
 * case of Spark Streaming the error is the difference between the measured processing
 * rate (number of elements/processing delay) and the previous rate.
 *
 * @see <a href="https://en.wikipedia.org/wiki/PID_controller">PID controller (Wikipedia)</a>
 *
 * @param batchIntervalMillis the batch duration, in milliseconds
 * @param proportional how much the correction should depend on the current
 *        error. This term usually provides the bulk of correction and should be positive or zero.
 *        A value too large would make the controller overshoot the setpoint, while a small value
 *        would make the controller too insensitive. The default value is 1.
 * @param integral how much the correction should depend on the accumulation
 *        of past errors. This value should be positive or 0. This term accelerates the movement
 *        towards the desired value, but a large value may lead to overshooting. The default value
 *        is 0.2.
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change. This value should be positive or 0.
 *        This term is not used very often, as it impacts stability of the system. The default
 *        value is 0.
 * @param minRate what is the minimum rate that can be estimated.
 *        This must be greater than zero, so that the system always receives some data for rate
 *        estimation to work.
 */
private[streaming] class PIDRateEstimator(
    batchIntervalMillis: Long,    // 每个批次的时间
    proportional: Double,     // conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
    integral: Double,       // onf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
    derivative: Double,   // conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
    minRate: Double       // conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
  ) extends RateEstimator with Logging {
  // 是否是第一次执行
  private var firstRun: Boolean = true
  // 上此计算完时间
  private var latestTime: Long = -1L
  // 上此速率
  private var latestRate: Double = -1D
  // 上此 error数量
  private var latestError: Double = -1L

  require(
    batchIntervalMillis > 0,
    s"Specified batch interval $batchIntervalMillis in PIDRateEstimator is invalid.")
  require(
    proportional >= 0,
    s"Proportional term $proportional in PIDRateEstimator should be >= 0.")
  require(
    integral >= 0,
    s"Integral term $integral in PIDRateEstimator should be >= 0.")
  require(
    derivative >= 0,
    s"Derivative term $derivative in PIDRateEstimator should be >= 0.")
  require(
    minRate > 0,
    s"Minimum rate in PIDRateEstimator should be > 0")

  logInfo(s"Created PIDRateEstimator with proportional = $proportional, integral = $integral, " +
    s"derivative = $derivative, min rate = $minRate")
  // 计算新的速率
  // time, elems, workDelay, waitDelay
  def compute(
      time: Long, // in milliseconds  // 处理的时间
      numElements: Long,
      processingDelay: Long, // in milliseconds  // workDelay
      schedulingDelay: Long // in milliseconds  // waitdelay
    ): Option[Double] = {
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {

        // in seconds, should be close to batchDuration
        // 从更新 到完成的 延迟时间
        val delaySinceUpdate = (time - latestTime).toDouble / 1000

        // in elements/second
        // 每秒的元素数,即处理的速率
        val processingRate = numElements.toDouble / processingDelay * 1000

        // In our system `error` is the difference between the desired rate and the measured rate
        // based on the latest batch information. We consider the desired rate to be latest rate,
        // which is what this estimator calculated for the previous batch.
        // in elements/second
        // 表示上此处理速率 和 本次速率的差值
        val error = latestRate - processingRate

        // The error integral, based on schedulingDelay as an indicator for accumulated errors.
        // A scheduling delay s corresponds to s * processingRate overflowing elements. Those
        // are elements that couldn't be processed in previous batches, leading to this delay.
        // In the following, we assume the processingRate didn't change too much.
        // From the number of overflowing elements we can calculate the rate at which they would be
        // processed by dividing it by the batch interval. This rate is our "historical" error,
        // or integral part, since if we subtracted this rate from the previous "calculated rate",
        // there wouldn't have been any overflowing elements, and the scheduling delay would have
        // been zero.
        // (in elements/second)
        // schedulingDelay.toDouble * processingRate 因为调度延迟 而 没有被处理掉的 记录
        // schedulingDelay.toDouble * processingRate/ batchIntervalMillis 就表示一个没有被处理的 历史速率
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        // error - latestError 本次速率差值 和 上此处理差值 的 差值
        // (error - latestError) / delaySinceUpdate  表示 delaySinceUpdate 段时间的 差值速率
        val dError = (error - latestError) / delaySinceUpdate
        // 新速率的计算方式
        // 最小为 minRate, 默认为100
        // delaySinceUpdate 默认为1   integral 默认为0.2  derivative默认为0  minRate默认为100
        // 可见主要作用是 proportional * error   微调作用为 integral * historicalError
        val newRate = (latestRate - proportional * error -
                                    integral * historicalError -
                                    derivative * dError).max(minRate)
        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)
        // 更新 记录
        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
  }
}
