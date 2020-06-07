package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp01 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp01")

        val ssc = new StreamingContext(conf, Seconds(5))

        val orderKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_ORDER_INFO), ssc)
        val detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL), ssc)
        val userKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_USER_INFO), ssc)

        userKafkaDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                val jedisClient: Jedis = RedisUtil.getJedisClient

                iter.foreach(record => {
                    val value: String = record.value()
                    val userInfo: UserInfo = JSON.parseObject( value , classOf[UserInfo])
                    val userRedisKey = s"user:${userInfo.id}"
                    jedisClient.set(userRedisKey , value)
                })
                jedisClient.close()
            })
        })

        val orderToInfoDStream: DStream[(String, OrderInfo)] = orderKafkaDStream.map(record => {
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            val timeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = timeArr(0)
            orderInfo.create_hour = timeArr(1).split(":")(0)

            val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
            orderInfo.consignee_tel = tuple._1 + "*******"

            (orderInfo.id, orderInfo)
        })

        val orderToDetailDStream: DStream[(String, OrderDetail)] = detailKafkaDStream.map(
            record => {
                val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
                (detail.order_id, detail)
            }
        )

        val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
            orderToInfoDStream.fullOuterJoin(orderToDetailDStream)

        val noUserSaleDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions(iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val saleDetails = new ListBuffer[SaleDetail]()
            implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

            iter.foreach { case (orderId, (infoOpt, detailOpt)) =>
                val orderRedisKey: String = s"order:${orderId}"
                val detailRedisKey = s"detail:${orderId}"

                if (infoOpt.isDefined) {
                    val orderInfo: OrderInfo = infoOpt.get

                    if (detailOpt.isDefined) {
                        val orderDetail: OrderDetail = detailOpt.get
                        val detail = new SaleDetail(orderInfo, orderDetail)
                        saleDetails += detail
                    }

                    val orderInfoStr: String = Serialization.write(orderInfo)
                    jedisClient.set(orderRedisKey, orderInfoStr)
                    jedisClient.expire(orderRedisKey, 120)

                    val details: util.Set[String] = jedisClient.smembers(detailRedisKey)
                    import scala.collection.JavaConversions._

                    details.foreach(orderDetailStr => {
                        val detail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                        val saleDetail = new SaleDetail(orderInfo, saleDetail)
                        saleDetails += saleDetail
                    })
                } else {
                    val orderDetail: OrderDetail = detailOpt.get
                    if (jedisClient.exists(orderRedisKey)) {
                        val orderInfoStr: String = jedisClient.get(orderRedisKey)
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                        val detail = new SaleDetail(orderInfo, orderDetail)
                        saleDetails += detail
                    } else {
                        val orderDetailStr: String = Serialization.write(orderDetail)
                        jedisClient.sadd(detailRedisKey, orderDetailStr)
                        jedisClient.expire(detailRedisKey, 120)
                    }
                }
            }
            jedisClient.close()
            saleDetails.toIterator
        })

        val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val list = new ListBuffer[SaleDetail]()

            iter.foreach(noUserSaleDetail => {
                val userRedisKey = s"user:${noUserSaleDetail.user_id}"

                if (jedisClient.exists(userRedisKey)) {
                    val userInfoStr: String = jedisClient.get(userRedisKey)
                    val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
                    noUserSaleDetail.mergeUserInfo(userInfo)
                }
                list += noUserSaleDetail
            })

            jedisClient.close()

            list.toIterator
        })

        saleDetailDStream.foreachRDD(rdd => {
            rdd.foreachPartition(
                iter => {
                    val date: String = new SimpleDateFormat("yyyy_MM-dd").format(new Date())

                    val docIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => {
                        val docId = s"${saleDetail.order_id}_${saleDetail.order_detail_id}"
                        (docId, saleDetail)
                    })

                    MyEsUtil.insertBulk(s"${GmallConstants.ES_SALE_DETAIL_INDEX}_$date" ,
                        docIdToSaleDetailIter.toList)
                }
            )
        })

        ssc.start()
        ssc.awaitTermination()
    }
}












