package cn.dev;


import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;
import scala.Tuple2;

import java.util.*;

public class Learn08 {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                config("spark.kryoserializer.buffer.max","1g").
                config("geospark.global.index","true").
                config("geospark.join.gridtype","quadtree").
                master("local[*]").appName("Learn08").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);

        String url = "jdbc:postgresql://192.168.10.174:5432/geospark";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","postgres");
        connectionProperties.put("password","root");
        connectionProperties.put("driver","org.postgresql.Driver");

        String table = "fwm_2018_500_gz";
        Dataset<Row> df = spark.read().jdbc(url, table, connectionProperties);
        df.createOrReplaceTempView("fwm_2018_500_gz");
        String sql = "select ST_GeomFromWKB(geom) as geom, `实际层数` as sjcs from fwm_2018_500_gz";
        df = spark.sql(sql);
        df.show();
        SpatialRDD<Geometry> fwmRDD = Adapter.toSpatialRdd(df, "geom");
        fwmRDD.analyze();
        fwmRDD.spatialPartitioning(GridType.KDBTREE);
        fwmRDD.buildIndex(IndexType.QUADTREE, true);


        table = "gz_grid_gz";
        df = spark.read().jdbc(url, table, connectionProperties);
        df.createOrReplaceTempView("gz_grid_gz");
        sql = "select ST_GeomFromWKB(geom),id as geom from gz_grid_gz";
        df = spark.sql(sql);

        List<String> fieldNames = Arrays.asList("id", "shape_area");
        SpatialRDD<Geometry> gridRDD = Adapter.toSpatialRdd(df, "geom");

        gridRDD.analyze();
        gridRDD.spatialPartitioning(fwmRDD.getPartitioner());

        JavaPairRDD<Geometry, HashSet<Geometry>> intersectRDD = JoinQuery.SpatialJoinQuery(fwmRDD, gridRDD, true, true);
        JavaPairRDD<Long, Double> r = intersectRDD.mapToPair(new PairFunction<Tuple2<Geometry, HashSet<Geometry>>, Long, Double>() {
            @Override
            public Tuple2<Long, Double> call(Tuple2<Geometry, HashSet<Geometry>> pair)  {
                    Geometry geo1 = pair._1;
                    Double sumArea = 0.0d;
                    for(Geometry geo : pair._2){
                        if(geo1.intersects(geo))
                            sumArea += geo.intersection(geo1).getArea();
                    }
                    return new Tuple2<>(Long.parseLong(geo1.getUserData().toString()), sumArea);
            }
        });
        r = r.sortByKey();
        JavaRDD<String> csvRDD = r.map(new Function<Tuple2<Long, Double>, String>() {
            @Override
            public String call(Tuple2<Long, Double> v1)  {
                return v1._1().toString() + "," + v1._2().toString();
            }
        });

        csvRDD.saveAsTextFile("./result");
    }
}
