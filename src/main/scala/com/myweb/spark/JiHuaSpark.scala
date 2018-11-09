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

object JiHuaSpark {
  Logger.getLogger("org").setLevel(Level.ERROR) //设置日志级别
  var confPath: String = System.getProperty("user.dir") + File.separator + "spark.properties"

  /**
    * 定义对象
    */
  case class JiHua(
                    id: String,
                    hanghao: String,
                    xiafajihuamingcheng: String,
                    xiafafangshi: String,
                    xiafabumen: String,
                    xiafasheng: String,
                    xiafashi: String,
                    huodongyue: String,
                    shifouchaijie: String,
                    chaijiefenshu: String,
                    xiafariqi: String,
                    xiafaliang: String,
                    mingdanleixing: String,
                    mingdanxiangshu: String,
                    youxianji: String
                  )

  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath)
    if (!file.exists()) {
      System.out.println(JiHuaSpark.getClass.getClassLoader.getResource("spark.properties"))
      val in = JiHuaSpark.getClass.getClassLoader.getResourceAsStream("spark.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(confPath))
    }
    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics.jihua")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)
    if (brokers == null || brokers == "" || topics == null || topics == "" || kuduMaster == null || kuduMaster == "") {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("JiHuaSpark")
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
        new JiHua(
          map.get("id").get.asInstanceOf[String],
          map.get("hanghao").get.asInstanceOf[String],
          map.get("xiafajihuamingcheng").get.asInstanceOf[String],
          map.get("xiafafangshi").get.asInstanceOf[String],
          map.get("xiafabumen").get.asInstanceOf[String],
          map.get("xiafasheng").get.asInstanceOf[String],
          map.get(" xiafashi").get.asInstanceOf[String],
          map.get(" huodongyue").get.asInstanceOf[String],
          map.get("shifouchaijie").get.asInstanceOf[String],
          map.get("chaijiefenshu").get.asInstanceOf[String],
          map.get("xiafariqi").get.asInstanceOf[String],
          map.get("xiafaliang").get.asInstanceOf[String],
          map.get("mingdanleixing").get.asInstanceOf[String],
          map.get(" mingdanxiangshu").get.asInstanceOf[String],
          map.get("youxianji").get.asInstanceOf[String]
        )
      })
      //将RDD转换为DataFrame
      val jihuaDF = sqlContext.createDataFrame(newrdd)
      println("Up Line" + brokers)
      kuduContext.upsertRows(jihuaDF, "jihua")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}