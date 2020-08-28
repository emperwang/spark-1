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

package org.apache.spark.streaming.kafka010

import java.{ util => ju }
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 *  A DStream where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 *  of messages
 * per second that each '''partition''' will accept.
 * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
 *   see [[LocationStrategy]] for more details.
 * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
 *   see [[ConsumerStrategy]] for more details
 * @param ppc configuration of settings such as max rate on a per-partition basis.
 *   see [[PerPartitionConfig]] for more details.
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
private[spark] class DirectKafkaInputDStream[K, V](
    _ssc: StreamingContext,
    locationStrategy: LocationStrategy,
    consumerStrategy: ConsumerStrategy[K, V],
    ppc: PerPartitionConfig
  ) extends InputDStream[ConsumerRecord[K, V]](_ssc) with Logging with CanCommitOffsets {
  // 初始拉取消息数
  private val initialRate = context.sparkContext.getConf.getLong(
    "spark.streaming.backpressure.initialRate", 0)
  // executor kakfa 的参数
  val executorKafkaParams = {
    val ekp = new ju.HashMap[String, Object](consumerStrategy.executorKafkaParams)
    // 重新设置了一下要发送给 executor的 kakfa配置
    KafkaUtils.fixKafkaParams(ekp)
    ekp
  }
  // 存储偏移信息
  protected var currentOffsets = Map[TopicPartition, Long]()

  @transient private var kc: Consumer[K, V] = null
  // 使用已经的consumer,或创建
  def consumer(): Consumer[K, V] = this.synchronized {
    if (null == kc) {
      // 这里会使用不同的消费策略来进行初始化,
      // 1. 创建KafkaConsumer
      // 2. 设置topic的 offset
      kc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava)
    }
    kc
  }

  override def persist(newLevel: StorageLevel): DStream[ConsumerRecord[K, V]] = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  protected def getBrokers = {
    val c = consumer
    val result = new ju.HashMap[TopicPartition, String]()
    val hosts = new ju.HashMap[TopicPartition, String]()
    val assignments = c.assignment().iterator()
    while (assignments.hasNext()) {
      val tp: TopicPartition = assignments.next()
      if (null == hosts.get(tp)) {
        val infos = c.partitionsFor(tp.topic).iterator()
        while (infos.hasNext()) {
          val i = infos.next()
          hosts.put(new TopicPartition(i.topic(), i.partition()), i.leader.host())
        }
      }
      result.put(tp, hosts.get(tp))
    }
    result
  }

  protected def getPreferredHosts: ju.Map[TopicPartition, String] = {
    locationStrategy match {
      case PreferBrokers => getBrokers
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) => hostMap
    }
  }

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka 0.10 direct stream [$id]"
  // 持久化的信息
  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
    // 速率控制器
  override protected[streaming] val rateController: Option[RateController] = {
      //是否开启了 反压机制
      // 即根据系统负载在决定 消费速率
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }
  // 每个分区数能获得的 最大消息数
  protected[streaming] def maxMessagesPerPartition(
    offsets: Map[TopicPartition, Long]): Option[Map[TopicPartition, Long]] = {
    val estimatedRateLimit = rateController.map { x => {
      val lr = x.getLatestRate()
      if (lr > 0) lr else initialRate
    }}

    // calculate a per-partition rate limit based on current lag
    // 2. 根据当前的lag 使用获取到的速率 设置每一个分区的速率
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        //  first_set_____________other message______________________latest
        // 此时计算 没有消费过的消息,即lag
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        // 总共的lag,各个分区的相加
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
          // 得到每个分区最大的值
          val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)
          // 计算每个分区的速率
          val backpressureRate = lag / totalLag.toDouble * rate
          // 如果maxRateLimitPerPartition 存在,取 maxRateLimitPerPartition和backpressureRate中比较小的
          // 如果不存在,则使用backpressureRate
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)} else backpressureRate)
        }
        // 没有合适的速率,则使用默认的速率 val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
      case None => offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp).toDouble }
    }
    // 3. 最终设置,防止没有设置
    if (effectiveRateLimitPerPartition.values.sum > 0) {
      // 每个批次是几秒
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
            // 再次计算 每个分区的 最大速率
            // 如果这里最大速率没有最小速率大,则使用最小速率minRate
            // val minRate = conf.getLong("spark.streaming.kafka.minRatePerPartition", 1)
        case (tp, limit) => tp -> Math.max((secsPerBatch * limit).toLong,
          ppc.minRatePerPartition(tp))
      })
    } else {
      None
    }
  }

  /**
   * The concern here is that poll might consume messages despite being paused,
   * which would throw off consumer position.  Fix position if this happens.
   */
    // 此次主要是再次设置每一个topic的offset为最小的
  private def paranoidPoll(c: Consumer[K, V]): Unit = {
    // don't actually want to consume any messages, so pause all partitions
      // 暂停消费所有的partition
    c.pause(c.assignment())
    // 消费消息
    val msgs = c.poll(0)
    if (!msgs.isEmpty) {
      // position should be minimum offset per topicpartition
      // 根据消费的消息,记录消息中对应的topic的partition的最小offset
      // 这里的acc用于存储最后的结果,和foldLeft 算子有关
      msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
        // 创建 TopicPartition
        val tp = new TopicPartition(m.topic, m.partition)
        // 先去acc中获取,并 和消费到的 offset对比, 选小的
        val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
        // 最后记录到 acc中
        acc + (tp -> off)
        // 下面的foreach 又是对acc的遍历
      }.foreach { case (tp, off) =>
          logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
        // 设置 topic的offset
        // 此处就是根据acc 设置topic的offset
          c.seek(tp, off)
      }
    }
  }

  /**
   * Returns the latest (highest) available offsets, taking new partitions into account.
   */
    // 获取最新的可用的partition对应的  offset
    // 感知动态的 分区信息
  protected def latestOffsets(): Map[TopicPartition, Long] = {
    val c = consumer
    paranoidPoll(c)
      // 获取 Set<TopicPartition>
    val parts = c.assignment().asScala

    // make sure new partitions are reflected in currentOffsets
      // 得到新加入的 partition
    val newPartitions = parts.diff(currentOffsets.keySet)

    // Check if there's any partition been revoked because of consumer rebalance.
      // 得到移除的partition
    val revokedPartitions = currentOffsets.keySet.diff(parts)
    if (revokedPartitions.nonEmpty) {
      throw new IllegalStateException(s"Previously tracked partitions " +
        s"${revokedPartitions.mkString("[", ",", "]")} been revoked by Kafka because of consumer " +
        s"rebalance. This is mostly due to another stream with same group id joined, " +
        s"please check if there're different streaming application misconfigure to use same " +
        s"group id. Fundamentally different stream should use different group id")
    }

    // position for new partitions determined by auto.offset.reset if no commit
    currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap

    // find latest available offsets
      // 把所有的topic设置到 最新位置
    c.seekToEnd(currentOffsets.keySet.asJava)
    parts.map(tp => tp -> c.position(tp)).toMap
  }

  // limits the maximum number of messages per partition
  protected def clamp(
    offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // 限制每个分区的最大消息数
    maxMessagesPerPartition(offsets).map { mmp =>
      mmp.map { case (tp, messages) =>
        // uo 在这里是最新的offset
          val uo = offsets(tp)
        // currentOffsets(tp) 是设置的offset(如果有的话)
        // currentOffsets(tp) + messages 就是一个endOffset,也就是根据速率算出来的最终此次任务可以消费到的位置
          tp -> Math.min(currentOffsets(tp) + messages, uo)
      }
    }.getOrElse(offsets)
  }
  // 此就是 rdd的计算了
  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {
    // 此是得到了每个partition 可以拉取的最大数量
    val untilOffsets = clamp(latestOffsets())
    // 此创建了一个 OffsetRange,其中包含了要拉取的partition的 start end区间
    val offsetRanges = untilOffsets.map { case (tp, uo) =>
      // 获取offset的开始位置
      val fo = currentOffsets(tp)
      // uo 是offset的结束位置
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }
    // 是否使用 consumerCache
    val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled",
      true)
    // 创建了一个KafkaRDD
    val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray,
      getPreferredHosts, useConsumerCache)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    // 1.过滤掉那些 offsetRanges是空的
    // 2.然后创建了一个字符串,其中包含了要消费的一些信息
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    // 把inputInfo注册 inputInfoTracker
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    currentOffsets = untilOffsets
    commitAll()
    Some(rdd)
  }
  // 开始
  override def start(): Unit = {
    // 创建consumer
    val c = consumer
    // 设置各个partition的offset为最小
    paranoidPoll(c)
    if (currentOffsets.isEmpty) {
      // 记录最先的 topic中partition中对应的offset
      currentOffsets = c.assignment().asScala.map { tp =>
        tp -> c.position(tp)
      }.toMap
    }
  }

  override def stop(): Unit = this.synchronized {
    if (kc != null) {
      kc.close()
    }
  }
  // 存储要提交的数据
  protected val commitQueue = new ConcurrentLinkedQueue[OffsetRange]
  protected val commitCallback = new AtomicReference[OffsetCommitCallback]

  /**
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   */
    // 提交操作
    // 这里的offset提交,其实就是把offset添加到了队列中
  def commitAsync(offsetRanges: Array[OffsetRange]): Unit = {
    commitAsync(offsetRanges, null)
  }

  /**
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   * @param callback Only the most recently provided callback will be used at commit.
   */
  def commitAsync(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit = {
    commitCallback.set(callback)
    commitQueue.addAll(ju.Arrays.asList(offsetRanges: _*))
  }
  // 真正提交 offset的地方
  protected def commitAll(): Unit = {
    // 创建一个容器,用于保存要提交的offset
    val m = new ju.HashMap[TopicPartition, OffsetAndMetadata]()
    // 从队列中获取offset
    var osr = commitQueue.poll()
    while (null != osr) {
      val tp = osr.topicPartition
      val x = m.get(tp)
      // 如果m中没有,则使用 osr.untilOffset
      // 如果m中已经存在了,则使用一个 值大的一个
      val offset = if (null == x) { osr.untilOffset } else { Math.max(x.offset, osr.untilOffset) }
      // 保存起来
      m.put(tp, new OffsetAndMetadata(offset))
      // 继续获取
      osr = commitQueue.poll()
    }
    // 如果存在offset呢
    // 那就进行提交了
    if (!m.isEmpty) {
      consumer.commitAsync(m, commitCallback.get)
    }
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time): Unit = {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time): Unit = { }

    override def restore(): Unit = {
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
         logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
         generatedRDDs += t -> new KafkaRDD[K, V](
           context.sparkContext,
           executorKafkaParams,
           b.map(OffsetRange(_)),
           getPreferredHosts,
           // during restore, it's possible same partition will be consumed from multiple
           // threads, so do not use cache.
           false
         )
      }
    }
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
}
