package cn.dev;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;

import java.lang.reflect.Array;
import java.util.Arrays;

public class Learn09 {
    public static void main(String[] args) throws Exception {
        // 初始化Spark
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("Learn09").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);


        spark.stop();
    }
}
