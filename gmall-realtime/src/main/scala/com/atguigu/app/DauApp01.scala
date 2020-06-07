package com.atguigu.app

/*
import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.phoenix.spark._
*/

object DauApp01 {

    def main(args: Array[String]): Unit = {

       /* //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf,Seconds(5))

        //3.消费Kafka启动日志主题数据创建流
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(Set(GmallConstants.KAFKA_TOPIC_STARTUP),ssc)

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

        //4.将每条数据转换为样例类对象,并添加logDate和logHour
        val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map {
            record =>
                //a.获取value,即传输过来的数据
                val value: String = record.value()

                //b.将value转换为样例类对象(缺少日期和小时)
                val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

                //c.获取时间戳
                val ts: Long = startUpLog.ts

                //d.格式化时间
                val dateHourStr: String = sdf.format(new Date(ts))
                val dateHourArr: Array[String] = dateHourStr.split(" ")

                //e.给样例类中日期及小时字段赋值
                startUpLog.logDate = dateHourArr(0)
                startUpLog.logHour = dateHourArr(1)

                //f.返回结果
                startUpLog
        }

        //startUpLogDStream.cache()
        //startUpLogDStream.count().print()

        val filterRedisDSream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

        //filterRedisDSream.cache()
        //filterRedisDSream.count().print()

        val filterByGroupDSream: DStream[StartUpLog] = DauHandler.filterByGroup(filterRedisDSream)

        filterByGroupDSream.cache()
        filterByGroupDSream.count().print()

        DauHandler.saveMidDtToRedis(filterByGroupDSream)

        filterByGroupDSream.foreachRDD(
            rdd => {
                rdd.saveToPhoenix("GMALL191125_DAU",
                    Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181"))
            }
        )

        ssc.start()
        ssc.awaitTermination()*/
    }

}
