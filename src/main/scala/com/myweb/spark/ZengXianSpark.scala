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

/**
  * 打包
  * mvn clean scala:compile package
  * 使用spark-submit的方式提交作业
  * spark-submit --class com.myweb.spark.ZengXianSpark \
  * --master yarn-client --num-executors 3 --driver-memory 1g \
  * --driver-cores 1 --executor-memory 1g --executor-cores 1 \
  * bd-spark-1.0-SNAPSHOT.jar
  */
object ZengXianSpark {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别
  var confPath: String = System.getProperty("user.dir") + File.separator + "zengxian.properties"

  /**
    * 定义对象
    */
  case class ZengXian(
                       id: String,
                       transno: String,
                       serviceid: String,
                       partners: String,
                       channel: String,
                       theme: String,
                       name: String,
                       phone: String,
                       sex: String,
                       birth: String,
                       idtype: String,
                       idno: String,
                       dataprovince: String,
                       datacity: String,
                       caddress: String,
                       datacounty: String,
                       sendlatestprd: String,
                       sendlatestdate: String,
                       czipcode: String,
                       relationship: String
                     )

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if (!file.exists()) {
      System.out.println(ZengXianSpark.getClass.getClassLoader.getResource("zengxian.properties"))
      val in = ZengXianSpark.getClass.getClassLoader.getResourceAsStream("zengxian.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }
    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)
    if (brokers == null || brokers == "" || topics == null || topics == "" || kuduMaster == null || kuduMaster == "") {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("ZengXianSpark")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //引入隐式
    val sqlContext = new SQLContext(sc)
    val kuduContext = new KuduContext(kuduMaster)
    dStream.foreachRDD(rdd => {
      //将rdd数据重新封装为Rdd[ZengXian]
      val newrdd = rdd.map(line => {
        println("Got Line" + brokers)
        val jsonObj = JSON.parseFull(line._2)
        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
        new ZengXian(
          map.get("id").get.asInstanceOf[String],
          map.get("transno").get.asInstanceOf[String],
          map.get("serviceid").get.asInstanceOf[String],
          map.get("partners").get.asInstanceOf[String],
          map.get("channel").get.asInstanceOf[String],
          map.get("theme").get.asInstanceOf[String],
          map.get("name").get.asInstanceOf[String],
          map.get("phone").get.asInstanceOf[String],
          map.get("sex").get.asInstanceOf[String],
          map.get("birth").get.asInstanceOf[String],
          map.get("idtype").get.asInstanceOf[String],
          map.get("idno").get.asInstanceOf[String],
          map.get("dataprovince").get.asInstanceOf[String],
          map.get("datacity").get.asInstanceOf[String],
          map.get("caddress").get.asInstanceOf[String],
          map.get("datacounty").get.asInstanceOf[String],
          map.get("sendlatestprd").get.asInstanceOf[String],
          map.get("sendlatestdate").get.asInstanceOf[String],
          map.get("czipcode").get.asInstanceOf[String],
          map.get("relationship").get.asInstanceOf[String]
        )
      })
      //将RDD转换为DataFrame
      val zengxianDF = sqlContext.createDataFrame(newrdd)
      println("Up Line" + brokers)
      kuduContext.upsertRows(zengxianDF, "zengxian")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}