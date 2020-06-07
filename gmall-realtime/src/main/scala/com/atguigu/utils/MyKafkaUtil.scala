package com.atguigu.utils

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MyKafkaUtil {

    //1.获取配置信息
    private val properties: Properties = PropertiesUtil.load("config.properties")

    //2.封装Kafka参数
    private val kafkaParam = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.group.id"),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    //3.消费Kafka主题获取数据创建流
    def getKafkaDStream(topics: Set[String], ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

        //消费数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))

        //返回结果
        kafkaDStream
    }

}
