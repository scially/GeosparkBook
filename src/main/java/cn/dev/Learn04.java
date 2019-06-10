package cn.dev;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;

public class Learn04 extends Object{
    public static void main(String[] args) throws Exception {
        //
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("cn.dev.Learn04").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        // 加载CSV文件,CSV中的第一列为WKT格式
        String inputCSVPath = Learn04.class.getResource("/county_small.tsv").toString();
        Dataset rawDF = spark.read().format("csv").
                option("delimiter", "\t").
                option("header", "false").
                load(inputCSVPath);
        rawDF.createOrReplaceTempView("rawdf");
        rawDF.show(10);

        // 创建Geometry列
        String sqlText = "select ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2, _c6 from rawdf";
        Dataset spatialDf  = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("spatialdf");
        spatialDf.show(10);
        spatialDf.printSchema();

        // 坐标系转换
        sqlText = "select ST_Transform(countyshape, 'epsg:4326', 'epsg:3857') as newcountyshape, countyshape, _c1, _c2, _c6 from spatialdf";
        spatialDf = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("spatialdf");
        spatialDf.show(10);

        // 范围查询
        sqlText = "select * from spatialdf where ST_contains(ST_PolygonFromEnvelope(-98.0,-97.0,111.0,111.0), countyshape)";
        spatialDf = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("spatialdf");
        spatialDf.show(10);

        // KNN临近查询
        sqlText = "select _c6 AS countyname, ST_Distance(ST_PointFromText('-98.0,111.0',','), countyshape) AS distance " +
                "from spatialdf " +
                "order by distance ASC " +
                "LIMIT 5";
        spatialDf = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("spatialdf");
        spatialDf.show();
        spark.stop();
    }
}
