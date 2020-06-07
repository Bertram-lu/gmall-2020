package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object joinTest {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("joinTest").setMaster("local[*]")

        val sc = new SparkContext(conf)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("a",1),("a",2),("b",1),("c",1)))
        val rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a",1),("b",1),("b",2),("d",1)))

        val result1: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        val result2: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        val result3: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
        val result4: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)

        result1.foreach(println)
        println("*********************")
        result2.foreach(println)
        println("*********************")
        result3.foreach(println)
        println("*********************")
        result4.foreach(println)

        sc.stop()
    }

}
