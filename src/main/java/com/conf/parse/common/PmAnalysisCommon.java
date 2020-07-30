package com.conf.parse.common;

import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class PmAnalysisCommon {


    public static void pmFileWrite(FileSystem fs, String filePath, Collection<?> lsobject) throws Exception {
        pmFileWrite(fs, filePath, lsobject, false);
    }

    public static void pmFileWrite(FileSystem fs, String filePath, Collection<?> lsobject, boolean isappend)
            throws Exception {
        try (OutputStream out = FSUtils.openOutputStream(fs, filePath, isappend);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out))) {

            // 为了性能考虑，不要在objectToString中生成SimpleDateFormat的实例
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (Object obj : lsobject) {
                bw.write(objectToString(obj, sdf));
            }
        }
    }


    private static String objectToString(Object o, SimpleDateFormat sdf) throws Exception {
        StringBuilder sb = new StringBuilder();
        String value = null;
        for (java.lang.reflect.Field f : o.getClass().getDeclaredFields()) {
            if (f.getType() == Date.class) {
                Object dateObject = f.get(o);
                value = sdf.format((Date) dateObject);
            } else {
                value = f.get(o) == null ? "" : f.get(o).toString();
            }
            sb.append(value);
            sb.append(',');
        }
        sb.append('\n');
        return sb.toString();
    }



}
