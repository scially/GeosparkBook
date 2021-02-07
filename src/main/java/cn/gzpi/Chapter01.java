package cn.gzpi;

import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

class Chapter01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Chapter01");
        conf.setMaster("local[*]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String pointRDDInputLocation = Chapter01.class.getResource("checkin.csv").toString();
        Integer pointRDDOffset = 0; // 地理位置(经纬度)从第0列开始
        FileDataSplitter pointRDDSplitter = FileDataSplitter.CSV;
        Boolean carryOtherAttributes = true; // 第二列的属性(酒店名)
        PointRDD rdd = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes);

        rdd.analyze();
        System.out.println(rdd.approximateTotalCount);

        rdd.CRSTransform("epsg:4326", "epsg:4547", true);
        rdd.rawSpatialRDD.foreach((p)->{
            System.out.println(p);
        });
    }
}
