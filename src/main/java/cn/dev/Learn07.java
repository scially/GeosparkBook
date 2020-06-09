package cn.dev;


import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;

import java.util.Properties;

public class Learn07 {
    public static void main(String[] args)  {
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("Learn07").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);

        String url = "jdbc:postgresql://192.168.10.174:5432/geospark";
        String table = "parks";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user","postgres");
        connectionProperties.put("password","root");
        connectionProperties.put("driver","org.postgresql.Driver");

        Dataset<Row> df = spark.read().jdbc(url, table, connectionProperties);
        df.createOrReplaceTempView("parks");
        df.show();

        // Before GeoSpark 1.3.0 要转为RDD,那么Geometery必须在第一列
        // GeoSpark 1.3.1 支持根据Geometry列名转RDD
        String sql = "select ST_GeomFromWKB(geom) as geom, parkname, parkid  from parks";
        df = spark.sql(sql);
        df.show();

        SpatialRDD<Geometry> rdd = Adapter.toSpatialRdd(df, "geom");
        rdd.rawSpatialRDD.foreach(geometry -> {
            System.out.println(geometry.toString());
        });
    }
}
