package cn.dev;

import org.geotools.data.*;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;


import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Learn10 {
    public static void main(String[] args) throws Exception {
        Charset sourceCharset = Charset.forName("GBK");
        Charset targetCharset = Charset.forName("UTF-8");
        Map<String, Object> parm = new HashMap<>();
        // 待转换源SHP文件
        parm.put("url", Learn10.class.getResource("/parks/Parks.shp"));
        ShapefileDataStore reader = (ShapefileDataStore)DataStoreFinder.getDataStore(parm);
        ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();

        Map<String, Serializable> dest = new HashMap<>();
        // 转换后目标SHP存储文件
        dest.put("url", new File("./Parks-UTF8.shp").toURL());
        ShapefileDataStore writer = (ShapefileDataStore)factory.createDataStore(dest);
        writer.createSchema(reader.getSchema());  // 复制源SHP的结构（包含坐标系）
        writer.setCharset(targetCharset);

        Transaction t = new DefaultTransaction("Add");
        SimpleFeatureStore featureStore = (SimpleFeatureStore)writer.getFeatureSource();
        featureStore.setTransaction(t);
        try {
            featureStore.addFeatures(reader.getFeatureSource().getFeatures());
            t.commit();
        }catch (Exception e){
            t.rollback();
        }finally {
            t.close();
        }
    }
}
