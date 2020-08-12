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

package org.apache.spark.scheduler

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    // 获取各自的 priority 其就是 jobId
    val priority1 = s1.priority
    val priority2 = s2.priority
    // 比较这两个优先级的大小
    // res = 1   pri1  大于 pri2
    // res = -1  pri1 小于 pri2
    // res = 0  相等
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      // 如果两个任务的优先级相等, 则比较其 stageId的大小
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    // 最小数 资源数 -- cpu 核数
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    // 运行的task 数
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    // 任务数 小于 最小资源数,则需要调度
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    // 最小分配的 比率
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0
    // 如果 s1 需要调度,s2不需要,则返回true
    if (s1Needy && !s2Needy) {
      return true
      // s2需要,返回false
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      // s1和s2都需要,则比较 shareRatio
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      // 否则比较 taskWeightRatio
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    // shareRatio1 小 或者  taskWeight1 小,则返回true
    if (compare < 0) {
      true
    } else if (compare > 0) {
      // ratio2 大,则返回false
      false
    } else {
      // 都相等,则比较名字
      s1.name < s2.name
    }
  }
}

