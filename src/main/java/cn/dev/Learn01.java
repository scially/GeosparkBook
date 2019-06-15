package cn.dev;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;

public class Learn01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("GeoSpark01");
        conf.setMaster("local[*]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Get SpatialRDD
        String pointRDDInputLocation = Learn01.class.getResource("/checkin.csv").toString();
        Integer pointRDDOffset = 0; // 地理位置(经纬度)从第0列开始
        FileDataSplitter pointRDDSplitter = FileDataSplitter.CSV;
        Boolean carryOtherAttributes = true; // 第二列的属性(酒店名)
        PointRDD rdd = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes);

        // 坐标系转换
        String sourceCrsCode = "epsg:4326";
        String targetCrsCode = "epsg:3857";
        rdd.CRSTransform(sourceCrsCode, targetCrsCode);

        sc.stop();
    }
}
