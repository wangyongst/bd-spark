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

object CeLueSpark {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别
  var confPath: String = System.getProperty("user.dir") + File.separator + "spark.properties"

  /**
    * 定义对象
    */
  case class CeLue(
                    id: String,
                    hanghao: String,
                    xiafaceluemingcheng: String,
                    xiafashi: String,
                    xiafabumen: String,
                    xiafasheng: String,
                    xiafajigou: String,
                    guishujigou: String,
                    huodongyue: String,
                    xiafaliang: String,
                    mingdanleixing: String,
                    mingdanxiangshu: String,
                    shujuguishusheng: String,
                    shujuguishushi: String,
                    dingshiqi: String,
                    youxiaoqijiezhi: String,
                    youxianji: String,
                    youxiaoqiqishi: String,
                    xiafajileiliang: String
                  )

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if (!file.exists()) {
      System.out.println(CeLueSpark.getClass.getClassLoader.getResource("spark.properties"))
      val in = CeLueSpark.getClass.getClassLoader.getResourceAsStream("spark.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }
    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics.celue")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)
    if (brokers == null || brokers == "" || topics == null || topics == "" || kuduMaster == null || kuduMaster == "") {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("CeLueSpark")
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
        new CeLue(
          map.get("id").get.asInstanceOf[String],
          map.get("hanghao").get.asInstanceOf[String],
          map.get("xiafaceluemingcheng").get.asInstanceOf[String],
          map.get("xiafashi").get.asInstanceOf[String],
          map.get("xiafabumen").get.asInstanceOf[String],
          map.get("xiafasheng").get.asInstanceOf[String],
          map.get("xiafajigou").get.asInstanceOf[String],
          map.get("guishujigou").get.asInstanceOf[String],
          map.get("huodongyue").get.asInstanceOf[String],
          map.get("xiafaliang").get.asInstanceOf[String],
          map.get("mingdanleixing").get.asInstanceOf[String],
          map.get("mingdanxiangshu").get.asInstanceOf[String],
          map.get("shujuguishusheng").get.asInstanceOf[String],
          map.get("shujuguishushi").get.asInstanceOf[String],
          map.get("dingshiqi").get.asInstanceOf[String],
          map.get("youxiaoqijiezhi").get.asInstanceOf[String],
          map.get("youxianji").get.asInstanceOf[String],
          map.get("youxiaoqiqishi").get.asInstanceOf[String],
          map.get("xiafajileiliang").get.asInstanceOf[String]
        )
      })
      //将RDD转换为DataFrame
      val celueDF = sqlContext.createDataFrame(newrdd)
      println("CeLue Up Line" + brokers)
      kuduContext.upsertRows(celueDF, "celue")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}