package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GvmApp01 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

        val ssc = new StreamingContext(conf,Seconds(5))

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_ORDER_INFO),ssc)

        val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(
            record => {
                val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

                val timeArr: Array[String] = orderInfo.create_time.split(" ")

                orderInfo.create_date = timeArr(0)
                orderInfo.create_hour = timeArr(1).split(":")(0)

                val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
                orderInfo.consignee_tel = tuple._1 + "*******"

                orderInfo
            }
        )

        orderInfoDStream.foreachRDD(
            rdd => {
                rdd.saveToPhoenix("GMALL191125_ORDER_INFO",Seq("ID", "PROVINCE_ID", "CONSIGNEE",
                    "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL",
                    "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO",
                    "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181"))
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
