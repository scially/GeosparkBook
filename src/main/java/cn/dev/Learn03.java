package cn.dev;

import cn.dev.Learn02;
import com.vividsolutions.jts.geom.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import java.util.HashSet;
import java.util.List;

public class Learn03 extends Object{
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("GeoSpark03");
        conf.setMaster("local[*]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Get SpatialRDD
        String shapeInputLocation = Learn02.class.getResource("/parks").toString();
        SpatialRDD parkRdd = ShapefileReader.readToGeometryRDD(sc, shapeInputLocation);

        // 构建索引
        boolean buildOnSpatialPartitionedRDD = false; // 如果只需要在做空间分析的时候构建索引,则设置为true
        parkRdd.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD);

        // 查询
        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(-123.1,49.2);
        coordinates[1] = new Coordinate(-123.1,49.3);
        coordinates[2] = new Coordinate(-123.2,49.3);
        coordinates[3] = new Coordinate(-123.2,29.2);
        coordinates[4] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
        Polygon polygonObject = geometryFactory.createPolygon(coordinates);
        boolean usingIndex = true;  // 使用索引
        JavaRDD<Geometry> queryResult = RangeQuery.SpatialRangeQuery(parkRdd, polygonObject, false, usingIndex);
        System.out.println(String.format("查询结果总数为: %d",queryResult.count()));

        // 临近查询(KNN)
        parkRdd.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD);  // QTREE不支持KNN查询
        Point pointObject = geometryFactory.createPoint(new Coordinate(-123.1,49.2));
        int K = 5; // K Nearest Neighbors
        List<Geometry> result = KNNQuery.SpatialKnnQuery(parkRdd, pointObject, K, usingIndex);
        // 输出结果
        System.out.println("距离点(-123.1,49.2)最近的五个公园是:");
        for (int i = 0; i < result.size(); i++){
            System.out.println(String.format("%d: %s",i+1, result.get(i).toString()));
        }


        // 空间关联查询
        shapeInputLocation = Learn02.class.getResource("/point").toString();
        SpatialRDD pointRdd = ShapefileReader.readToGeometryRDD(sc, shapeInputLocation);
        // analyze方法主要用来计算边界
        pointRdd.analyze();
        parkRdd.analyze();
        // spark中的分区操作,空间关联查询前必须进行
        parkRdd.spatialPartitioning(GridType.KDBTREE);
        pointRdd.spatialPartitioning(parkRdd.getPartitioner());

        boolean considerBoundaryIntersection = true;  // 因为是以点为基础,因此必须考虑边界
        usingIndex = false;
        JavaPairRDD<Geometry, HashSet<Geometry>> joinResult = JoinQuery.SpatialJoinQuery(parkRdd, pointRdd, usingIndex, considerBoundaryIntersection);
        System.out.println("空间关联查询结果为:");
        joinResult.foreach((kv)->{
            System.out.println(String.format("{%s}--{%s}", kv._1.getUserData().toString(), kv._2.toString()));
        });
        sc.stop();
    }
}
