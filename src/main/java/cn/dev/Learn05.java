package cn.dev;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;
import org.datasyslab.geosparkviz.utils.ImageType;


public class Learn05 {
    public static void main(String[] args) throws Exception {
        //
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("cn.dev.Learn05").getOrCreate();

        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);
        // 加载CSV文件,CSV中的第一列为WKT格式
        String inputCSVPath = Learn04.class.getResource("/checkin.csv").toString();
        Dataset rawDF = spark.read().format("csv").
                option("delimiter", ",").
                option("header", "false").
                load(inputCSVPath);
        rawDF.createOrReplaceTempView("pointtable");

        // 创建Geometry列
        String sqlText = "select ST_Point(cast(_c0 as Decimal(24,20)), cast(_c1 as Decimal(24,20))) AS shape, _c2 from pointtable";
        Dataset spatialDf  = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("pointtable");
        spatialDf.show();

        sqlText = "SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable";
        spatialDf  = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("boundtable");
        spatialDf.show();

        sqlText = "SELECT pixel, shape FROM pointtable " +
                "LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel";
        spatialDf  = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("pixels");
        spatialDf.show();

//        sqlText = "select pixel, count(*) as weight from pixels group by pixel";
//        spatialDf = spark.sql(sqlText);
//        spatialDf.createOrReplaceTempView("pixelaggregates");
//        spatialDf.show();

        sqlText = "SELECT ST_Colorize(1, 1, 'red') as color, pixel FROM pixels";
        spatialDf = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("pixelaggregates");
        spatialDf.show(false);

        sqlText = "SELECT ST_Render(pixel, color) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary FROM pixelaggregates" ;
        spatialDf = spark.sql(sqlText);
        spatialDf.createOrReplaceTempView("images");
        spatialDf.show();

        Dataset<org.apache.spark.sql.Row> images = spark.table("images");
        Row[] take = (Row[])images.take(1);
        ImageSerializableWrapper image = (ImageSerializableWrapper)take[0].get(0);
        new ImageGenerator().SaveRasterImageAsLocalFile(image.getImage(),System.getProperty("user.home") + "/point", ImageType.PNG);

        spark.stop();
    }
}
