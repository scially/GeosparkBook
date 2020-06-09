package cn.dev.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.spatialRDD.PointRDD

object Learn01 extends App{
  val conf = new SparkConf
  conf.setAppName("GeoSpark01")
  conf.setMaster("local[*]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator")
  val sc = new SparkContext(conf)

  val pointRDDInputLocationn = this.getClass.getResource("/checkin.csv").toString
  val pointRDDOffset = 0
  val pointRDDSplitter = FileDataSplitter.CSV
  val carryOtherAttributes = true

  //  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)
  //  Scala提供了从SparkContext到JavaSparkContext的隐士转化
  val rdd = new PointRDD(sc, pointRDDInputLocationn, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)

  // 坐标转换
  rdd.CRSTransform("EPSG:4326", "EPSG:4457")

  // rdd.rawSpatialRDD中是JavaRDD,若用Scala,需要RDD,否则会出现很多参数匹配错误
  rdd.rawSpatialRDD.rdd.foreach(println)
}
