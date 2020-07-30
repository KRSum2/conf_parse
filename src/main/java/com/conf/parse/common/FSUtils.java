package com.conf.parse.common;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;

public class FSUtils {

    private static final Logger log = MiscUtils.getLogger(FSUtils.class);

    public static final Charset utf8 = Charset.forName("UTF-8");
    public static final int buffSize = 2 * 1024 * 1024; // 2m

    public static final String LZO = ".lzo";

    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
    }

    public static FileSystem getNewFileSystem() throws IOException {
        Configuration conf = new Configuration();
        // conf.set("fs.hdfs.impl.disable.cache", "true");
        return FileSystem.newInstance(conf);
    }

    public static byte[] readDataFile(FileSystem fs, String dataFile) throws IOException {
        Path p = new Path(dataFile);
        return readDataFile(fs, p);
    }

    public static byte[] readDataFile(FileSystem fs, Path dataFile) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream(2 * 1024 * 1024);
        InputStream in = null;
        try {
            in = fs.open(dataFile);
            IOUtils.copyBytes(in, bos, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }

        return bos.toByteArray();
    }

    public static ArrayList<String> getAllFiles(FileSystem fs, String path) throws Exception {
        return getAllFiles(fs, new Path(path));
    }

    public static ArrayList<String> getAllFiles(FileSystem fs, Path path) throws Exception {
        ArrayList<String> files = new ArrayList<>();
        if (fs.isFile(path))
            files.add(path.toString());
        else
            getFiles(fs, path, files);
        return files;
    }

    private static void getFiles(FileSystem fs, Path dir, ArrayList<String> files) throws Exception {
        for (FileStatus fileStatus : fs.listStatus(dir)) {
            if (fileStatus.isDirectory())
                getFiles(fs, fileStatus.getPath(), files);
            else
                files.add(fileStatus.getPath().toString());
        }
    }

    public static JavaRDD<String> getJavaRDD(FileSystem fs, JavaSparkContext sc, String path) throws Exception {
        Path p = new Path(path);
        if (fs.isFile(p)) {
            return sc.textFile(path);
        }

        boolean containsDir = false;
        for (FileStatus fileStatus : fs.listStatus(p)) {
            if (fileStatus.isDirectory()) {
                containsDir = true;
                break;
            }
        }
        if (!containsDir) {
            JavaRDD<String> rdd = null;
            try {
                rdd = sc.textFile(path); // 有可能是lzo压缩后的目录，普通目录会直接抛异常
                return rdd;
            } catch (Exception e) {
            }
        }

        ArrayList<String> files = FSUtils.getAllFiles(fs, path);
        if (files.isEmpty()) {
            log.info("在" + path + "目录下未找到任何文件");
            return null;
        }

        JavaRDD<String> rdd = null;
        for (String file : files) {
            FileStatus fileStatus = fs.getFileStatus(new Path(file));
            if (fileStatus.getLen() <= 0)
                continue;
            if (rdd == null)
                rdd = sc.textFile(file);
            else
                rdd = rdd.union(sc.textFile(file));
        }

        return rdd;
    }

    public static boolean isNotEmptyFile(FileSystem fs, Path file) throws Exception {
        FileStatus fileStatus = fs.getFileStatus(file);
        return fileStatus.getLen() > 0;
    }

    public static ArrayList<String> getDataFiles(FileSystem fs, String path) throws Exception {
        ArrayList<String> files = new ArrayList<>();
        Path p = new Path(path);
        if (fs.isFile(p)) {
            files.add(path);
            return files;
        }

        boolean containsDir = false;
        boolean containsLzo = false;
        for (FileStatus fileStatus : fs.listStatus(p)) {
            if (fileStatus.isDirectory()) {
                containsDir = true;
                break;
            }

            if (fileStatus.getPath().toString().endsWith(".lzo")) {
                containsLzo = true;
                break;
            }
        }
        if (!containsDir && containsLzo) {
            files.add(path); // 有可能是lzo压缩后的目录，普通目录会直接抛异常
            return files;
        }

        files = getAllFiles(fs, path);
        if (files.isEmpty()) {
            log.info("在" + path + "目录下未找到任何文件");
            return files;
        }

        ArrayList<String> files2 = new ArrayList<>();
        for (String file : files) {
            FileStatus fileStatus = fs.getFileStatus(new Path(file));
            if (fileStatus.getLen() > 0) {
                files2.add(file);
            }
        }

        return files2;
    }

    public static FSDataOutputStream openOutputStream(FileSystem fs, String path) throws Exception {
        return openOutputStream(fs, new Path(path), true);
    }

    public static FSDataOutputStream openOutputStream(FileSystem fs, Path path) throws Exception {
        return openOutputStream(fs, path, true);
    }

    public static FSDataOutputStream openOutputStream(FileSystem fs, String path, boolean append) throws Exception {
        return openOutputStream(fs, new Path(path), append);
    }

    public static FSDataOutputStream openOutputStream(FileSystem fs, Path path, boolean append) throws Exception {
        FSDataOutputStream os = null;
        if (!append) {
            os = fs.create(path, true);
        } else {
            if (fs.exists(path)) {
                try {
                    os = fs.append(path);
                } catch (Exception e) {
                    // 不支持append时，先读出来，再写回去
                    byte[] oldBytes = FSUtils.readDataFile(fs, path);
                    os = fs.create(path);
                    os.write(oldBytes);
                }
            } else {
                os = fs.create(path);
            }
        }

        return os;
    }

    public static void closeStreamSilently(java.io.Closeable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (Throwable t) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to close stream", t);
                }
            }
        }
    }

    public static boolean isDirectory(FileSystem fs, String p) throws Exception {
        return fs.isDirectory(new Path(p));
    }

    public static boolean isFile(FileSystem fs, String p) throws Exception {
        return fs.isFile(new Path(p));
    }

    public static boolean exists(FileSystem fs, String p) throws Exception {
        return fs.exists(new Path(p));
    }

    public static boolean exists(FileSystem fs, Path p) throws Exception {
        return fs.exists(p);
    }

    public static boolean existsPartitions(FileSystem fs, String partitionDir) throws Exception {
        Path p = new Path(partitionDir);
        boolean exists = true;
        if (!fs.exists(p)) {
            fs.mkdirs(p);
            exists = false;
        } else {
            FileStatus[] files = fs.listStatus(p);
            if (files == null || files.length == 0) {
                exists = false;
            }
        }
        return exists;
    }

    public static boolean isRCFile(String file) {
        return file != null && file.endsWith(".rcfile");
    }

    public static boolean isLzoFile(String file) {
        return file != null && file.endsWith(LZO);
    }

    public static RCFileBufferedReaderIterable createRCFileBufferedReaderIterable(FileSystem fs, String file,
                                                                                  List<Integer> colIds) throws IOException {
        return new RCFileBufferedReaderIterable(fs, file, colIds);
    }

    public static InputStream openInputStream(FileSystem fs, String file) throws IOException {
        Path path = new Path(file);

        CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
        CompressionCodec codec = factory.getCodec(path);

        InputStream inputStream = fs.open(path, 8096);

        if (codec != null) {
            inputStream = codec.createInputStream(inputStream);
        }
        return inputStream;
    }

    public static class BufferedReaderIterable implements Iterable<String>, Closeable {

        private final long startTime = System.currentTimeMillis();
        private final String file;
        private final BufferedReader br;
        private final long size;
        private long vaildRecords;

        public BufferedReaderIterable(FileSystem fs, String file) throws IOException {
            // this.file = file;
            // Path path = new Path(file);
            // FSDataInputStream in = fs.open(path, 8096);
            // this.br = new BufferedReader(new InputStreamReader(in, FSUtils.utf8));
            // this.size = fs.getFileStatus(path).getLen();

            this.file = file;
            Path path = new Path(file);
            this.size = fs.getFileStatus(path).getLen();

            CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
            CompressionCodec codec = factory.getCodec(path);

            FSDataInputStream inputStream = fs.open(path, 8096);

            if (codec == null) {
                br = new BufferedReader(new InputStreamReader(inputStream, FSUtils.utf8));
            } else {
                CompressionInputStream comInputStream = codec.createInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(comInputStream));
            }
        }

        public long size() {
            return size;
        }

        public void incrementVaildRecords() {
            vaildRecords++;
        }

        public long getVaildRecords() {
            return vaildRecords;
        }

        public void info(Logger log, String action) {
            info(log, action, false);
        }

        public void info(Logger log, String action, boolean second) {
            String timeStr;
            if (second)
                timeStr = (System.currentTimeMillis() - startTime) / 1000 + "s";
            else
                timeStr = (System.currentTimeMillis() - startTime) + "ms";
            log.info(action + ": " + file + ", file size: " + MiscUtils.prettyPrintMemoryOrDiskSize(size)
                    + ", records: " + vaildRecords + ", total time: " + timeStr);
        }

        public static void error(Logger log, String action, String file, Exception e) {
            log.error("Failed to " + action + ": " + file, e);
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private String line;

                @Override
                public boolean hasNext() {
                    try {
                        line = br.readLine();
                    } catch (IOException e) {
                        log.error("Failed to readLine, file: " + file, e);
                        line = null;
                    }
                    return line != null;
                }

                @Override
                public String next() {
                    return line;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        @Override
        public void close() throws IOException {
            closeStreamSilently(br);
        }
    }

    public static class RCFileBufferedReaderIterable implements Iterable<String[]>, Closeable {

        private final long startTime = System.currentTimeMillis();
        private final String file;
        private final long size;
        private long vaildRecords;

        private final RCFile.Reader reader;
        private final LongWritable rowID = new LongWritable();
        private final BytesRefArrayWritable cols = new BytesRefArrayWritable();
        private final int colCount;
        private final List<Integer> colIds;

        public RCFileBufferedReaderIterable(FileSystem fs, String file, List<Integer> colIds) throws IOException {
            this.file = file;
            Path path = new Path(file);
            this.size = fs.getFileStatus(path).getLen();
            colCount = colIds.size();
            this.colIds = colIds;

            Configuration conf = new Configuration(fs.getConf());
            ColumnProjectionUtils.appendReadColumns(conf, colIds);
            reader = new RCFile.Reader(fs, new Path(file), conf);
        }

        public long size() {
            return size;
        }

        public void incrementVaildRecords() {
            vaildRecords++;
        }

        public long getVaildRecords() {
            return vaildRecords;
        }

        public void info(Logger log, String action) {
            info(log, action, false);
        }

        public void info(Logger log, String action, boolean second) {
            String timeStr;
            if (second)
                timeStr = (System.currentTimeMillis() - startTime) / 1000 + "s";
            else
                timeStr = (System.currentTimeMillis() - startTime) + "ms";
            log.info(action + ": " + file + ", file size: " + MiscUtils.prettyPrintMemoryOrDiskSize(size)
                    + ", records: " + vaildRecords + ", total time: " + timeStr);
        }

        public static void error(Logger log, String action, String file, Exception e) {
            log.error("Failed to " + action + ": " + file, e);
        }

        @Override
        public Iterator<String[]> iterator() {
            return new Iterator<String[]>() {
                @Override
                public boolean hasNext() {
                    try {
                        return reader.next(rowID);
                    } catch (IOException e) {
                        log.error("Failed to execute hasNext, file: " + file, e);
                        return false;
                    }

                }

                @Override
                public String[] next() {
                    String[] a = new String[colCount];
                    try {
                        reader.getCurrentRow(cols);
                        BytesRefWritable brw = null;
                        for (int i = 0; i < colCount; i++) {
                            brw = cols.get(colIds.get(i));
                            a[i] = new String(brw.getData(), brw.getStart(), brw.getLength());
                        }
                    } catch (IOException e) {
                        log.error("Failed to execute next, file: " + file, e);
                    }
                    return a;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    public static int parallelize(int size, int parallelThreads, ArrayList<int[]> startAndEndIndexList) {
        int avg = size / parallelThreads;
        if (avg <= 0) {
            startAndEndIndexList.add(new int[]{0, size});
            return 1;
        }

        if (size % parallelThreads != 0)
            avg++;

        for (int i = 0; i < parallelThreads; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (end > size)
                end = size;
            startAndEndIndexList.add(new int[]{start, end});
        }
        return parallelThreads;
    }

    public static ArrayList<int[]> parallelize(int size, int parallelThreads) {
        ArrayList<int[]> list = new ArrayList<>();
        int avg = size / parallelThreads;

        if (size % parallelThreads != 0)
            avg++;

        for (int i = 0; i < parallelThreads; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (end > size)
                end = size;
            list.add(new int[]{start, end});
        }
        return list;
    }

    private static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
    private static final boolean IS_WINDOWS = OPERATING_SYSTEM.contains("windows");

    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    public static boolean isNotWritingFile(File f) {
        if (IS_WINDOWS)
            return true; // windows平台不支持lsof命令，直接过滤掉

        try {
            String[] cmdA = {"lsof", f.toString()};
            Process process = Runtime.getRuntime().exec(cmdA);
            LineNumberReader br = new LineNumberReader(new InputStreamReader(process.getInputStream()));
            String line = br.readLine();
            br.close();
            return line == null;
        } catch (Exception e) {
            log.error("Failed to exec lsof: " + f, e);
        }
        return true;
    }

    public static String getUID() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            ip = "UnknownHost";
        }
        ip += "_" + UUID.randomUUID();
        ip = ip.replace('.', '_').replace(':', '_').replace('-', '_');
        return ip;
    }

    public static void getDataFilesRecursively(FileSystem fs, String p, ArrayList<DataFile> files, String type, String dateTime) throws Exception {
        getDataFilesRecursively(fs, new Path(p), files, type, dateTime);
    }


    public static boolean isLoadFile(String type, Path path, String dateTime) {
        String rule="";

        if (type.equals("PM_2G_HW")) {
            rule = dateTime.substring(8, 10) + "30";
        } else if (type.equals("PM_2G_ZTE")) {
            rule = dateTime;
        } else if (type.equals("PM_2G_AL")) {
            rule = dateTime.substring(2,10);
        } else if (type.equals("PM_4G_HW")) {
            rule = dateTime.substring(0, 8) + "." + dateTime.substring(8, 10);
        } else if (type.equals("PM_4G_ZTE")) {
            rule = dateTime.substring(0, 8) + "_" + dateTime.substring(8, 10);
        } else if (type.equals("PM_4G_ERIC")) {
            rule = dateTime;
        }else {
            return  false;
        }

        if (path.getName().contains(rule)) {
            return true;
        }else
            return false;

    }


    public static void getDataFilesRecursively(FileSystem fs, Path p, ArrayList<DataFile> files, String type, String dateTime) throws Exception {
        if (fs.isFile(p)) {
            if (isNotEmptyFile(fs, p)){
                files.add(new DataFile(p.toString()));
                System.out.println(p.toString());
            }

            return;
        }

        boolean containsLzo = false;
        for (FileStatus fileStatus : fs.listStatus(p)) {
            Path path = fileStatus.getPath();
            if (fs.isFile(path) && DataFile.isLzoFile(path)) {
                containsLzo = true;
                break;
            }
        }
        // 如果当前目录下有lzo文件，那么不再递归遍历子目录，并且只抽取lzo文件
        if (containsLzo) {
            for (FileStatus fileStatus : fs.listStatus(p)) {
                Path lzoPath = fileStatus.getPath();
                if (fs.isFile(lzoPath) && DataFile.isLzoFile(lzoPath) && isNotEmptyFile(fs, lzoPath)) {
                    files.add(new DataFile(lzoPath, true));
                    System.out.println(lzoPath.toString());
                }
            }
        } else {
            for (FileStatus fileStatus : fs.listStatus(p)) {
                Path path = fileStatus.getPath();
                if (fs.isFile(path)) {
                    //即使是文件,判断文件名和类型相互匹配结果

                    String rule;

                    if (isNotEmptyFile(fs, path)) {
                        //对文件进行判别筛选
                        if (isLoadFile(type, path, dateTime)) {
                            files.add(new DataFile(path, false));
                            System.out.println(path.toString());
                        }
                    }

                } else {
                    //进行判断文件夹的名字+类型 综合判断是否继续读该文件夹下的内容  这里应该用continue
                    if ((path.getName().equals("itf_n") && type.equals("PM_2G_HW")) || (path.getName().equals("GD") && type.equals("PM_4G_ZTE"))) {
                        continue;
                    }
                    //针对时间目录,如果匹配不上时间目录不进行递归调用,直接continue
                    if ((path.getName().contains("pmexport_") && type.equals("PM_2G_HW")) || (path.getName().contains("neexport_") && type.equals("PM_4G_HW"))) {
                        if (!(path.getName().contains(dateTime.substring(0, 8)))) {
                            continue;
                        }
                    }

                    getDataFilesRecursively(fs, path, files, type, dateTime);
                }
            }
        }
    }

    public static BufferedReaderIterable createBufferedReaderIterable(FileSystem fs, String file) throws IOException {
        return new BufferedReaderIterable(fs, file);
    }
    // public static OutputStream createLzoOutputStream(FileSystem fs, OutputStream os) throws IOException {
    // // 要用LzopCodec，而不是LzoCodec，
    // // 否则com.wellcommsoft.noce.common.DataFile.getRdd(JavaSparkContext)那里会报错: Invalid lzo header
    // LzopCodec lzopCodec = new LzopCodec();
    // lzopCodec.setConf(fs.getConf());
    // os = lzopCodec.createOutputStream(os);
    // return os;
    // }

    // public static OutputStream createLzoOutputStream(FileSystem fs, Path lzoFile) throws IOException {
    // OutputStream os = fs.create(lzoFile);
    // return createLzoOutputStream(fs, os);
    // }
    //
    // public static OutputStream createLzoOutputStream(FileSystem fs, String lzoFile) throws IOException {
    // return createLzoOutputStream(fs, new Path(lzoFile));
    // }

}
