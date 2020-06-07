package com.atguigu.handler

import java.{lang, util}
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 同批次去重
    *
    * @param filterByRedisDStream 经过Redis过滤后的数据集
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.转换数据结构===> startUpLog->((mid,logDate),startUpLog)
    val midAndDtToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startUpLog =>
      ((startUpLog.mid, startUpLog.logDate), startUpLog)
    )

    //2.分组
    val midAndDtToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDtToLogDStream.groupByKey()

    //3.对迭代器排序并取第一条数据
    val midAndDtToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDtToLogIterDStream.mapValues(iter =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    )

    //4.提取数据
    val result: DStream[StartUpLog] = midAndDtToLogListDStream.flatMap {
      case ((mid, date), logs) => logs
    }

    //合并3.4
    //    midAndDtToLogIterDStream.flatMap { case ((_, _), iter) =>
    //      iter.toList.sortWith(_.ts < _.ts).take(1)
    //    }

    //5.返回结果
    result
  }


  /**
    * 使用Redis对数据做跨批次去重
    *
    * @param startUpLogDStream 原始数据集
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：每一条数据都会获取连接并释放
    //    val value1: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
    //      //a.获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.查询Redis中是否存在该mid
    //      val redisKey = s"dau:${startUpLog.logDate}"
    //      val boolean: lang.Boolean = !jedisClient.sismember(redisKey, startUpLog.mid)
    //      //c.回收Redis连接
    //      jedisClient.close()
    //      //d.返回结果
    //      boolean
    //    })

    //方案二：每个分区获取一次连接
    //    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
    //      //a.获取连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.过滤
    //      val filterLogs: Iterator[StartUpLog] = iter.filter { startUpLog =>
    //        val redisKey = s"dau:${startUpLog.logDate}"
    //        !jedisClient.sismember(redisKey, startUpLog.mid)
    //      }
    //      //c.释放连接
    //      jedisClient.close()
    //      //d.返回结果
    //      filterLogs
    //    })

    //方案三：每一个批次获取一次Redis数据,并广播至Executor用于数据过滤
    //1---->Driver端,全局只执行一次
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //2---->Driver端,每个批次执行一次

      //a.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis,获取mids(使用当前时间会有什么问题)
      val mids: util.Set[String] = jedisClient.smembers(s"dau:${LocalDate.now.toString}")
      //c.广播变量
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      //d.释放连接
      jedisClient.close()

      //e.过滤数据
      rdd.filter(log => !midsBC.value.contains(log.mid))
    })

    //    value1
    //    value2
    value3

  }


  /**
    * 将两次去重后的结果写入Redis
    *
    * @param startUpLogDStream 经过两次去重后的结果数据集
    */
  def saveMidDtToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {

    startUpLogDStream.foreachRDD { rdd =>

      //对当前RDD中每一个分区进行写库操作
      rdd.foreachPartition(iter => {
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历写出
        iter.foreach(startUpLog => {
          val redisKey: String = s"dau:${startUpLog.logDate}"
          jedisClient.sadd(redisKey, startUpLog.mid)
        })
        //c.关闭连接
        jedisClient.close()
      })
    }
  }
}
