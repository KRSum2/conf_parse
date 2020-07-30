package com.work.common;

import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class DataFile implements Serializable {

    public final String name;
    public final boolean isLzoFile;

    private boolean snappyCodec;

    public boolean deleteSrcFilesOnEtled;

    public String ip;

    public DataFile(String name) {
        this.name = name;
        this.isLzoFile = isLzoFile(name);
    }

    public DataFile(String name, boolean isLzoFile) {
        this.name = name;
        this.isLzoFile = isLzoFile(name);
    }

    public DataFile(Path file, boolean isLzoFile) {
        this.name = file.toString();
        this.isLzoFile = isLzoFile;
    }

    public boolean isSnappyCodec() {
        return snappyCodec;
    }

    public void setSnappyCodec(boolean snappyCodec) {
        this.snappyCodec = snappyCodec;
    }

    public JavaRDD<String> getRdd(JavaSparkContext sc) {
        if (snappyCodec) {
            // Configuration jobConf = new Configuration();
            // jobConf.set("io.compression.codecs",
            // "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec,"
            // + "org.apache.hadoop.io.compress.GzipCodec," + "org.apache.hadoop.io.compress.BZip2Codec,"
            // + "com.hadoop.compression.lzo.LzoCodec," + "com.hadoop.compression.lzo.LzopCodec");

            @SuppressWarnings("unchecked")
            JavaPairRDD<LongWritable, BytesRefArrayWritable> inputRDD = sc.hadoopFile(name, RCFileInputFormat.class,
                    LongWritable.class, BytesRefArrayWritable.class);

            return inputRDD.map(new Function<Tuple2<LongWritable, BytesRefArrayWritable>, String>() {
                @Override
                public String call(Tuple2<LongWritable, BytesRefArrayWritable> t) throws Exception {
                    BytesRefArrayWritable cols = t._2;
                    StringBuilder buff = new StringBuilder();
                    for (int i = 0, size = cols.size(); i < size; i++) {
                        if (i != 0) {
                            buff.append("\u0001");
                        }
                        BytesRefWritable brw = cols.get(i);
                        // 根据start和 length获取指定行-列数据
                        buff.append(new String(brw.getData(), brw.getStart(), brw.getLength()));
                    }
                    return buff.toString();
                }
            });
        } else if (!isLzoFile) {
            return sc.textFile(name);
        }
        return null;

        // Configuration jobConf = new Configuration();
        // jobConf.set("io.compression.codecs",
        // "org.apache.hadoop.io.compress.DefaultCodec," + "org.apache.hadoop.io.compress.GzipCodec,"
        // + "org.apache.hadoop.io.compress.BZip2Codec," + "com.hadoop.compression.lzo.LzoCodec,"
        // + "com.hadoop.compression.lzo.LzopCodec");
        // // jobConf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        // // jobConf.set("mapred.output.compress", "true");
        // // jobConf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        //
        // JavaPairRDD<LongWritable, Text> inputRDD = sc.newAPIHadoopFile(name, LzoTextInputFormat.class,
        // LongWritable.class, Text.class, jobConf);
        //
        // return inputRDD.map(new Function<Tuple2<LongWritable, Text>, String>() {
        // @Override
        // public String call(Tuple2<LongWritable, Text> t) throws Exception {
        // return t._2.toString();
        // }
        // });
    }

    @Override
    public String toString() {
        return "DataFile[" + name + "]";
    }

/*    public long getModificationTime(FileSystem fs) throws Exception {
        if (fs.exists(new Path(name)))
            return fs.getFileStatus(new Path(name)).getModificationTime();
        else if (fs.exists(new Path(name + ETL.ETLED)))
            return fs.getFileStatus(new Path(name + ETL.ETLED)).getModificationTime();
        return 0;
    }

    public void delete(FileSystem fs) throws Exception {
        if (fs.exists(new Path(name + ETL.ETLED)))
            fs.delete(new Path(name + ETL.ETLED), false);
    }*/

    public static boolean isLzoFile(Path file) {
        return isLzoFile(file.getName());
    }

    public static boolean isLzoFile(String file) {
        return file.toLowerCase().endsWith(".lzo");
    }

}
