package cn.dev;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;
import org.datasyslab.geosparkviz.utils.ImageType;


public class Learn06 {
    public static void main(String[] args) throws Exception {
        //
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("cn.dev.Learn06").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);

        // SpatialRDD转为DataFrame
        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
        String inputPath = Learn06.class.getResource("/parks").toString();
        SpatialRDD<Geometry> rdd = ShapefileReader.readToGeometryRDD(new JavaSparkContext(spark.sparkContext()), inputPath);

        Dataset<Row> rawDF = Adapter.toDf(rdd, spark);
        rawDF.createOrReplaceTempView("park");
        rawDF.show();
        rawDF.printSchema();

        // 构建几何图形(Geometry)
        String sqlText = "select ST_GeomFromWKT(geometry) as shape, * from park";
        rawDF = spark.sql(sqlText);
        rawDF.createOrReplaceTempView("park");
        rawDF.show();
        rawDF.printSchema();

        // 转为像素
        sqlText = "select ST_Envelope_Aggr(shape) as boundary from park";
        rawDF = spark.sql(sqlText);
        rawDF.createOrReplaceTempView("bound");

        sqlText = "select ST_Pixelize(shape, 256, 256, (select boundary from bound)) as pixel, shape from park ";
        rawDF = spark.sql(sqlText);
        rawDF.createOrReplaceTempView("pixels");
        rawDF.show(false);
        // 选择颜色
        sqlText = "select pixel, shape, ST_Colorize(1, 1, 'red') as color from pixels";
        rawDF = spark.sql(sqlText);
        rawDF.createOrReplaceTempView("pixels");
        rawDF.show();
        // 渲染
        sqlText = "select ST_Render(pixel, color) as image from pixels";
        rawDF = spark.sql(sqlText);
        rawDF.createOrReplaceTempView("images");
        rawDF.show();
        // 保存
        Dataset<org.apache.spark.sql.Row> images = spark.table("images");
        Row[] take = (Row[])images.take(1);
        ImageSerializableWrapper image = (ImageSerializableWrapper)take[0].get(0);
        new ImageGenerator().SaveRasterImageAsLocalFile(image.getImage(),System.getProperty("user.home") + "/park", ImageType.PNG);

        spark.stop();
    }
}
