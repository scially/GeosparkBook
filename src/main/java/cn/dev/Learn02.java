package cn.dev;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

public class Learn02 extends Object{
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("GeoSpark02");
        conf.setMaster("local[*]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Get SpatialRDD
        String shapeInputLocation = Learn02.class.getResource("/parks").toString();
        SpatialRDD rdd = ShapefileReader.readToGeometryRDD(sc, shapeInputLocation);

        // Envelop
        Envelope rangeQueryWindow = new Envelope(-123.1, -123.2, 49.2, 49.3);
        boolean considerBoundaryIntersection = false;// Only return gemeotries fully covered by the window
        boolean usingIndex = false;
        JavaRDD<Geometry> queryResult = RangeQuery.SpatialRangeQuery(rdd, rangeQueryWindow, considerBoundaryIntersection, usingIndex);
        System.out.println(String.format("查询结果总数为: %d",queryResult.count()));

        // Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(-123.1,49.2);
        coordinates[1] = new Coordinate(-123.1,49.3);
        coordinates[2] = new Coordinate(-123.2,49.3);
        coordinates[3] = new Coordinate(-123.2,29.2);
        coordinates[4] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
        Polygon polygonObject = geometryFactory.createPolygon(coordinates);
        queryResult = RangeQuery.SpatialRangeQuery(rdd, polygonObject, considerBoundaryIntersection, usingIndex);
        System.out.println(String.format("查询结果总数为: %d",queryResult.count()));

        // 遍历查询结果
        queryResult.foreach(new VoidFunction<Geometry>() {
            @Override
            public void call(Geometry geometry) throws Exception {
                System.out.println(geometry);
            }
        });
        sc.stop();
    }
}
