package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

    def main(args: Array[String]): Unit = {

        //TODO 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

        //TODO 创建StreamingContext
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 读取Kafka中订单信息以及订单详情主题数据
        val orderKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_ORDER_INFO) , ssc)
        val detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL) , ssc)
        val userKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_USER_INFO) , ssc)

        //TODO  将用户信息缓存至Redis
        userKafkaDStream.foreachRDD(
            rdd => {
                //对RDD的每一个分区进行操作,减少连接的获取与释放
                rdd.foreachPartition(
                    iter => {
                        //a.获取连接
                        val jedisClient: Jedis = RedisUtil.getJedisClient

                        //b.遍历分区数据,写入Redis
                        iter.foreach(
                            record => {
                                val value: String = record.value()
                                val userInfo: UserInfo = JSON.parseObject(value,classOf[UserInfo])
                                val userRedisKey = s"user:${userInfo.id}"
                                jedisClient.set(userRedisKey,value)
                            }
                        )

                        //c.释放连接
                        jedisClient.close()
                    }
                )
            }
        )

        //TODO 将orderKafkaDStream和detailKafkaDStream转换为元组(orderId,样例类)
        val idToInfoDstream: DStream[(String, OrderInfo)] = orderKafkaDStream.map(
            record => {
                //a.转换为样例类对象
                val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
                //b.处理时间
                val timeArr: Array[String] = orderInfo.create_time.split(" ")
                orderInfo.create_date = timeArr(0)
                orderInfo.create_hour = timeArr(1).split(":")(0)

                //c.处理手机号
                val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
                orderInfo.consignee_tel = tuple._1 + "*******"

                //d.返回结果
                (orderInfo.id, orderInfo)
            }
        )

        val orderToDetailDStream: DStream[(String, OrderDetail)] = detailKafkaDStream.map(
            record => {
                //a.转换为样例类对象
                val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
                //b.返回结果
                (detail.order_id, detail)
            }
        )

        //TODO 双流JOIN(FullJoin)
        val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
            idToInfoDstream.fullOuterJoin(orderToDetailDStream)


        //TODO 使用mapPartitions代替map操作,减少连接的创建与释放
        val noUserSaleDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions(
            iter => {
                //a.分区内部操作,获取连接,定义集合,转换样例类为String
                val jedisClient: Jedis = RedisUtil.getJedisClient
                val saleDetails = new ListBuffer[SaleDetail]()
                implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

                //b.对一个分区数据遍历处理
                iter.foreach { case (orderId, (infoOpt, detailOpt)) =>
                    val orderRedisKey: String = s"order:$orderId"
                    val detailRedisKey = s"detail:$orderId"

                    //判断infoOpt是否有数据
                    if (infoOpt.isDefined) {
                        //1.info数据不为空
                        // 取出info数据
                        val orderInfo: OrderInfo = infoOpt.get

                        //1.1 判断detailOpt是否为空
                        if (detailOpt.isDefined) {
                            val orderDetail: OrderDetail = detailOpt.get
                            val detail = new SaleDetail(orderInfo, orderDetail)
                            saleDetails += detail
                        }

                        //1.2 将orderInfo转换为JSON串并写入Redis
                        //val infoStr: String = JSON.toJSONString(orderInfo) 不行,编译报错
                        val orderInfoStr: String = Serialization.write(orderInfo)
                        jedisClient.set(orderRedisKey, orderInfoStr)
                        jedisClient.expire(orderRedisKey, 120)

                        //1.3 查询Detail缓存是否有数据,如果有数据,结合写入集合
                        val details: util.Set[String] = jedisClient.smembers(detailRedisKey)
                        //用于Java和Scala集合的转换
                        import scala.collection.JavaConversions._
                        details.foreach(
                            orderDetailStr => {
                                val detail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                                val saleDetail = new SaleDetail(orderInfo, detail)
                                saleDetails += saleDetail
                            }
                        )

                        //2.info数据为空
                    } else {
                        //取出OrderDetail数据
                        val orderDetail: OrderDetail = detailOpt.get

                        //查询Redis中时候存在对应的INFO数据
                        if (jedisClient.exists(orderRedisKey)) {

                            //2.1 存在对应的info数据,结合存入集合
                            val orderInfoStr: String = jedisClient.get(orderRedisKey)
                            val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                            val detail = new SaleDetail(orderInfo, orderDetail)
                            saleDetails += detail
                        } else {

                            //2.2 不存在,将自身写入Redis
                            val orderDetailStr: String = Serialization.write(orderDetail)
                            jedisClient.sadd(detailRedisKey, orderDetailStr)
                            jedisClient.expire(detailRedisKey, 120)

                        }
                    }
                }

                //c.释放连接
                jedisClient.close()

                //d.返回结果
                saleDetails.toIterator
            }
        )
            //普通JOIN

//        val noUserSaleDetailDStream: DStream[SaleDetail] = idToInfoDstream.join(orderToDetailDStream).map {
//            case (_, (orderInfo, orderDetail)) =>
//                new SaleDetail(orderInfo, orderDetail)
//        }
        //测试打印
        noUserSaleDetailDStream.print(100)

        //TODO 查询Redis,补充noUserSaleDetailDStream中的用户信息
        val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(
            iter => {
                //a.获取连接
                val jedisClient: Jedis = RedisUtil.getJedisClient
                val list = new ListBuffer[SaleDetail]()

                //b.对分区遍历,进行操作
                iter.foreach(noUserSaleDetail => {
                    val userRedisKey = s"user:${noUserSaleDetail.user_id}"

                    if (jedisClient.exists(userRedisKey)) {
                        val userInfoStr: String = jedisClient.get(userRedisKey)
                        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
                        noUserSaleDetail.mergeUserInfo(userInfo)
                    }

                    list += noUserSaleDetail
                })

                //c.释放连接
                jedisClient.close()

                //d.返回结果
                list.toIterator
            }
        )

        //TODO 将三个流JOIN的结果写入ES
        saleDetailDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                        val docIdToSaleDEtailIter: Iterator[(String, SaleDetail)] = iter.map(
                            saleDetail => {
                                val docId = s"${saleDetail.order_id}_${saleDetail.order_detail_id}"
                                (docId, saleDetail)
                            }
                        )
                        MyEsUtil.insertBulk(s"${GmallConstants.ES_SALE_DETAIL_INDEX}_$date" ,
                            docIdToSaleDEtailIter.toList)
                    }
                )
            }
        )

        //TODO 开启任务
        ssc.start()
        ssc.awaitTermination()
    }
}
