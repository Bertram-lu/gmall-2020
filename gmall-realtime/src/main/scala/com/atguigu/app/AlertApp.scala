package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
        val sdf2 = new SimpleDateFormat("HH:mm")

        //3.读取Kafka事件日志主题数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream( Set(GmallConstants.KAFKA_TOPIC_EVENT) , ssc)

        //4.将每一行数据转换成样例类对象
        val eventLogDStream: DStream[EventLog] = kafkaDStream.map(
            record => {
                //a.将value转换为样例类对象
                val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
                //b.获取时间戳
                val ts: Long = eventLog.ts
                //c.格式化时间戳,赋值日期和小时字段
                val dateHourStr: String = sdf.format(new Date(ts))
                val dateHourArr: Array[String] = dateHourStr.split(" ")
                eventLog.logDate = dateHourArr(0)
                eventLog.logHour = dateHourArr(1)

                //d.返回数据
                eventLog
            }
        )

        //5.开窗
        val windowDStream: DStream[EventLog] = eventLogDStream.window(Seconds(30))

        //6.分组
        val midToLogDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(
            log => (log.mid, log)
        ).groupByKey()

        //7.组内处理数据：三次及以上用不同账号登录并领取优惠劵
        // 并且过程中没有浏览商品
        val boolToAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogDStream.map {
            case (mid, iter) =>
                //定义一个集合用于存放领券的UID
                val uids = new util.HashSet[String]()

                //定义一个集合用于存放优惠券涉及的商品
                val items = new util.HashSet[String]()

                //定义一个集合用于存放所有事件
                val events = new util.ArrayList[String]()

                //定义标记,表示是否存在浏览商品行为
                var noClick: Boolean = true

                breakable {
                    iter.foreach(
                        log => {
                            //添加事件至events
                            events.add(log.evid)
                            if ("coupon".equals(log.evid)) {
                                //领券行为,则添加uid以及添加商品ID
                                uids.add(log.uid)
                                items.add(log.itemid)
                            } else if ("clickItem".equals(log.evid)) {
                                noClick = false
                                break()
                            }
                        }
                    )
                }
                //生成疑似预警日志
                (uids.size() >= 3 && noClick, CouponAlertInfo(mid,
                    uids,
                    items,
                    events,
                    System.currentTimeMillis))
        }
        

        //8.根据是否是超过3个UID领券以及是否有浏览商品行为做过滤
        //    boolToAlertDStream.cache()
        //    boolToAlertDStream.print()
        val alertInfoDStream: DStream[CouponAlertInfo] = boolToAlertDStream.filter(_._1).map(_._2)

        //9.打印测试,处理数据(去重)写入ES
        //    alertInfoDStream.print()
        //调整数据结构  mid_29-14:26
        val docIdToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfoDStream.map(
            log => {
                val ts: Long = log.ts
                val hourMinu: String = sdf2.format(new Date(ts))

                (s"${log.mid}-$hourMinu", log)
            }
        )
        docIdToAlertInfoDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val date: String = sdf.format(new Date()).split(" ")(0)
                        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEX + "_" + date, iter.toList)
                    }
                )
            }
        )

        //10.开启任务
        ssc.start()
        ssc.awaitTermination()
    }
}
