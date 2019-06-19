package cn.dev;

import com.sun.xml.internal.fastinfoset.sax.Features;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.RasterOverlayOperator;
import org.datasyslab.geosparkviz.extension.imageGenerator.GeoSparkVizImageGenerator;
import org.datasyslab.geosparkviz.extension.visualizationEffect.ChoroplethMap;
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;
import org.datasyslab.geosparkviz.utils.ImageType;
import org.datasyslab.geosparkviz.utils.Pixel;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;
import scala.Tuple2;

import java.awt.*;
import java.util.*;
import java.util.List;

public class Dev {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().
                config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                config("spark.kryoserializer.buffer.max", "1g").
                config("geospark.global.index", "true").
                config("geospark.join.gridtype", "quadtree").
                master("local[*]").appName("Dev").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);

        String url = "jdbc:postgresql://192.168.10.174:5432/geospark";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "org.postgresql.Driver");

        String table = "fwm_2018_500_gz";
        Dataset<Row> df = spark.read().jdbc(url, table, connectionProperties);
        df.createOrReplaceTempView("fwm_2018_500_gz");
        String sql = "select ST_GeomFromWKB(geom) as geom, `实际层数` as sjcs from fwm_2018_500_gz ";
        df = spark.sql(sql);

        SpatialRDD<Geometry> fwmRDD = Adapter.toSpatialRdd(df, "geom");
        fwmRDD.analyze();
        fwmRDD.spatialPartitioning(GridType.KDBTREE);
        fwmRDD.buildIndex(IndexType.QUADTREE, true);

        table = "gz_grid_gz";
        df = spark.read().jdbc(url, table, connectionProperties);
        df.createOrReplaceTempView("gz_grid_gz");
        sql = "select ST_GeomFromWKB(geom),id as geom from gz_grid_gz";
        df = spark.sql(sql);

        SpatialRDD<Geometry> gridRDD = Adapter.toSpatialRdd(df, "geom");
        sql = "select ST_Envelope_Aggr(ST_GeomFromWKB(geom)) as bound from gz_grid_gz";
        df = spark.sql(sql);
        Row[] row = (Row[])(df.take(1));
        Geometry bound = row[0].getAs(0);
        gridRDD.analyze();
        gridRDD.spatialPartitioning(fwmRDD.getPartitioner());

        JavaPairRDD<Geometry, HashSet<Geometry>> intersectRDD = JoinQuery.SpatialJoinQuery(fwmRDD, gridRDD, true, true);
        JavaPairRDD<Geometry, Double> r = intersectRDD.mapToPair(new PairFunction<Tuple2<Geometry, HashSet<Geometry>>, Geometry, Double>() {
            @Override
            public Tuple2<Geometry, Double> call(Tuple2<Geometry, HashSet<Geometry>> pair) {
                Geometry geo1 = pair._1;

                Double sumArea = 0d;
                for (Geometry geo : pair._2) {
                    if (geo1.intersects(geo))
                        sumArea += geo.intersection(geo1).getArea();
                }
                return new Tuple2<>(geo1, sumArea);
            }
        });

        JavaPairRDD<Geometry, Double> r2 = r.rightOuterJoin(gridRDD.rawSpatialRDD.mapToPair(new PairFunction<Geometry, Geometry, Double>() {
            @Override
            public Tuple2<Geometry, Double> call(Geometry geometry) throws Exception {
                return new Tuple2<>(geometry, 0d);
            }
        })).mapToPair(new PairFunction<Tuple2<Geometry, Tuple2<Optional<Double>, Double>>, Geometry, Double>() {
            @Override
            public Tuple2<Geometry, Double> call(Tuple2<Geometry, Tuple2<Optional<Double>, Double>> pair) throws Exception {
                Double area = pair._2._1.isPresent() ? pair._2._1.get() : pair._2._2;
                return new Tuple2<>(pair._1, area);
            }
        });

        JavaRDD<Geometry> r3 = r2.map(new Function<Tuple2<Geometry, Double>, Geometry>() {
            @Override
            public Geometry call(Tuple2<Geometry, Double> v1) throws Exception {
                v1._1.setUserData(v1._2);
                return v1._1;
            }
        });
        r3 = r3.repartition(1);
        r3.mapPartitions(new FlatMapFunction<Iterator<Geometry>, String>() {
            @Override
            public Iterator<String> call(Iterator<Geometry> iterator) throws Exception {
                ArrayList<String> result = new ArrayList();
                GeoJSONWriter writer = new GeoJSONWriter();
                List<Feature> features = new ArrayList<>();
                while (iterator.hasNext()) {
                    Geometry spatialObject = (Geometry) iterator.next();
                    Feature jsonFeature;
                    if (spatialObject.getUserData() != null) {
                        Map<String, Object> userData = new HashMap<String, Object>();
                        userData.put("UserData", spatialObject.getUserData());
                        jsonFeature = new Feature(writer.write(spatialObject), userData);
                    }
                    else {
                        jsonFeature = new Feature(writer.write(spatialObject), null);
                    }
                    features.add(jsonFeature);
                }
                GeoJSON jsonstring = writer.write(features);
                //String jsonstring = jsonFeature.toString();
                result.add(jsonstring.toString());
                return result.iterator();
            }
        }).saveAsTextFile("./result");
    }
}