package com.atguigu.utils

import java.io
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.codehaus.jackson.map.deser.std.StringDeserializer

object MyKafkaUtil02 {

    /*//获取配置参数
    private val properties: Properties = PropertiesUtil.load("config.properties")

    //封装Kafka参数
    private val kafkaParam: Map[String, io.Serializable] = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.group.id"),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )


    def getkafkaDStream(topics: Set[String] , ssc : StreamingContext) = {

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParam)
        )
        kafkaDStream
    }*/
}
