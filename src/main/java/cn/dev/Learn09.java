package cn.dev;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Learn09 {
    public static void main(String[] args) throws Exception {
        InputStream dbf = Learn09.class.getResourceAsStream("/point/point.dbf");
        byte[] bytes = new byte[30];
        dbf.read(bytes);
        byte b = bytes[29];
        System.out.println(Integer.toHexString(Byte.toUnsignedInt(b)));
    }
}
