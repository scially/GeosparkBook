package cn.dev;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

import java.lang.reflect.Array;
import java.util.Arrays;

public class Learn08 {
    public static void main(String[] args) throws Exception {
        // 初始化Spark
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("Learn08").getOrCreate();


        // 加载CSV文件
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String pointRDDInputLocation = Learn08.class.getResource("/checkin.csv").toString();
        Integer pointRDDOffset = 0; // 地理位置(经纬度)从第0列开始
        FileDataSplitter pointRDDSplitter = FileDataSplitter.CSV;
        Boolean carryOtherAttributes = true; // 第二列的属性(酒店名)，这里我们要加载的字段

        PointRDD rdd = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes);
        rdd.rawSpatialRDD.foreach((point -> {
            String[] attrs = point.getUserData().toString().split("\t");
            System.out.println(StringUtils.join(attrs, "|"));
        }));

        spark.stop();
    }
}
