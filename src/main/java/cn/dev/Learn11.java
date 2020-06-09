package cn.dev;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper;
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator;
import org.datasyslab.geosparkviz.utils.ImageType;

import java.util.Iterator;
import java.util.stream.Stream;

public class Learn11{
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().
                config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                config("spark.kryo.registrator", "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator").
                master("local[*]").appName("Learn11").getOrCreate();


        GeoSparkSQLRegistrator.registerAll(spark);
        GeoSparkVizRegistrator.registerAll(spark);

        String inputCSVPath = Learn11.class.getResource("/test/point.tsv").toString();
        spark.read().format("csv").
                option("delimiter", "\t").
                option("header", "false").
                load(inputCSVPath)
                .createOrReplaceTempView("point");

        spark.sql("CREATE OR REPLACE TEMP VIEW point as select st_geomfromtext(_c0) _c0 from point");

        inputCSVPath = Learn11.class.getResource("/test/polygon.tsv").toString();
        spark.read().format("csv").
                option("delimiter", "\t").
                option("header", "false").
                load(inputCSVPath).
                createOrReplaceTempView("polygon");

        spark.sql("CREATE OR REPLACE TEMP VIEW polygon as select st_geomfromtext(_c0) _c0 from polygon");
        spark.sql("CREATE OR REPLACE TEMP VIEW bound AS SELECT ST_Envelope_Aggr(_c0) as bound FROM polygon");
        spark.sql("CREATE OR REPLACE TEMP VIEW pixels AS SELECT pixel, _c0 FROM polygon LATERAL VIEW ST_Pixelize(_c0, 256, 256, (select * from bound)) AS pixel");
        spark.sql("CREATE OR REPLACE TEMP VIEW pixelaggregates AS SELECT pixel, ST_Colorize(1, 1, 'red') as color FROM pixels");
        spark.sql("CREATE OR REPLACE TEMP VIEW images AS SELECT ST_Render(pixel, color) AS image FROM pixelaggregates");
        Row[] rows = (Row[])spark.table("images").take(1);
        ImageSerializableWrapper image = (ImageSerializableWrapper)rows[0].get(0);
        new ImageGenerator().SaveRasterImageAsLocalFile(image.getImage(),System.getProperty("user.home") + "/point", ImageType.PNG);
        //spark.sql("insert into pixels SELECT pixel, _c0 FROM polygon LATERAL VIEW ST_Pixelize(_c0, 256, 256, (select * from bound)) AS pixel");
        //spark.sql("SELECT * from point, polygon where ST_Contains(polygon._c0,point._c0)");
    }
}
