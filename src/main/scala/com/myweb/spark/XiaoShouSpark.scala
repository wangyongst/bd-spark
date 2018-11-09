package com.myweb.spark

import java.io.{File, FileInputStream}
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object XiaoShouSpark {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别
  var confPath: String = System.getProperty("user.dir") + File.separator + "spark.properties"

  /**
    * 定义对象
    */
  case class XiaoShou(
                       id: String,
                       hanghao: String,
                       shujuguishujigou: String,
                       shujuguishusheng: String,
                       shujuguishushi: String,
                       mingdanleixing: String,
                       mingdanxiangshu: String,
                       shoujihaoma: String,
                       xingming: String,
                       xingbie: String,
                       shengri: String,
                       nianling: String,
                       shujulaiyuan: String,
                       yewumoshi: String,
                       fugaileixing: String,
                       beizhu: String
                     )

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if (!file.exists()) {
      System.out.println(XiaoShouSpark.getClass.getClassLoader.getResource("spark.properties"))
      val in = XiaoShouSpark.getClass.getClassLoader.getResourceAsStream("spark.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }
    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics.xiaoshou")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)
    if (brokers == null || brokers == "" || topics == null || topics == "" || kuduMaster == null || kuduMaster == "") {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("XiaoShouSpark")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //引入隐式
    val sqlContext = new SQLContext(sc)
    val kuduContext = new KuduContext(kuduMaster)
    dStream.foreachRDD(rdd => {
      val newrdd = rdd.map(line => {
        println("Got Line" + brokers)
        val jsonObj = JSON.parseFull(line._2)
        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        new XiaoShou(
          map.get("id").get.asInstanceOf[String],
          map.get("hanghao").get.asInstanceOf[String],
          map.get("shujuguishujigou").get.asInstanceOf[String],
          map.get("shujuguishusheng").get.asInstanceOf[String],
          map.get(" shujuguishushi").get.asInstanceOf[String],
          map.get("mingdanleixing").get.asInstanceOf[String],
          map.get("mingdanxiangshu").get.asInstanceOf[String],
          map.get("shoujihaoma").get.asInstanceOf[String],
          map.get("xingming").get.asInstanceOf[String],
          map.get("xingbie").get.asInstanceOf[String],
          map.get("shengri").get.asInstanceOf[String],
          map.get("nianling").get.asInstanceOf[String],
          map.get("shujulaiyuan").get.asInstanceOf[String],
          map.get("yewumoshi").get.asInstanceOf[String],
          map.get("fugaileixing").get.asInstanceOf[String],
          map.get("beizhu").get.asInstanceOf[String]
        )
      })
      //将RDD转换为DataFrame
      val xiaoshouDF = sqlContext.createDataFrame(newrdd)
      println("Up Line" + brokers)
      kuduContext.upsertRows(xiaoshouDF, "xiaoshou")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}