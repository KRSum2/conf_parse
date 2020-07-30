package com.conf.parse;

import com.conf.parse.common.FSUtils;
import com.conf.parse.common.MiscUtils;
import com.conf.parse.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;


public class InitTestEnvironment {

    private static final Logger log = MiscUtils.getLogger(InitTestEnvironment.class);
    private static final String CLASS_NAME = InitTestEnvironment.class.getSimpleName();

    public static void setDerbyHome() {
        // hive默认会用到derby，这里配置一下derby.system.home参数，
        // 可以把metastore_db和derby.log转到./target/hive/目录中
        System.setProperty("derby.system.home", "./target/hive/");
    }

    public static void main(String[] args) throws Exception {
        setDerbyHome();

        FileSystem fs = null;
        JavaSparkContext sc = null;
        try {
            fs = FSUtils.getNewFileSystem();
            Config.dev_test_mode = true;

            deleteDirs(fs);
            //copyFiles(fs);
            // copyFiles2(fs);
            // copyFiles3(fs);

            SparkConf sparkConf = new SparkConf();
            sparkConf.setAppName(CLASS_NAME);
            sparkConf.setMaster("local");
            sc = new JavaSparkContext(sparkConf);
            HiveContext sqlContext = new HiveContext(sc);
            createFile(fs);
            createTables(fs, sqlContext);
        } catch (Throwable t) {
            log.error(CLASS_NAME + ".run exception", t);
        } finally {
            try {
                if (sc != null)
                    sc.close();
            } catch (Exception e) {
                log.error("Failed to close JavaSparkContext", e);
            }
            try {
                if (fs != null)
                    fs.close();
            } catch (Exception e) {
                log.error("Failed to close FileSystem", e);
            }
        }
    }

    //创建hdfs目录
    private static void createFile(FileSystem fs) throws Exception {
        Path path = new Path(Config.getHdfsRootPath());
        if(!FSUtils.exists(fs, path)){
            fs.mkdirs(path);
        }
    }

    private static synchronized void deleteDirs(FileSystem fs) throws Exception {
        deleteDir(fs, "collector");
        deleteDir(fs, "copied_files");
        deleteDir(fs, "local");
        deleteDir(fs, "logs");
        deleteDir(fs, "hdfs");
        deleteDir(fs, "hive");
    }

    private static synchronized void deleteDir(FileSystem fs, String dir) throws Exception {
        Path p = new Path("./target/" + dir);
        if (fs.exists(p)) {
            log.info("delete " + p);
            fs.delete(p, true);
        }
    }

    private static synchronized void copyFiles3(FileSystem fs) throws Exception {
        Path localFile = new Path("./src/Test/resources/etl/etl_zte_pm_formula.csv");
        Path to = new Path("./target/hdfs/ETL/ETL_PM_FORMULA/etl_zte_pm_formula.csv");
        log.info("copy local file " + localFile + " to " + to);
        fs.copyFromLocalFile(localFile, to);
    }

    private static synchronized void copyFiles2(FileSystem fs) throws Exception {
        Path localFile = new Path("./src/Test/resources/etl/etl_HW_pm_formula.csv");
        Path to = new Path("./target/hdfs/ETL/ETL_PM_FORMULA/etl_HW_pm_formula.csv");
        log.info("copy local file " + localFile + " to " + to);
        fs.copyFromLocalFile(localFile, to);
    }

    private static synchronized void copyFiles(FileSystem fs) throws Exception {
        Path localFile = new Path("./src/Test/resources/etl/etl_pm_formula.csv");
        Path to = new Path("./target/hdfs/ETL/ETL_PM_FORMULA/etl_pm_formula.csv");
        log.info("copy local file " + localFile + " to " + to);
        fs.copyFromLocalFile(localFile, to);
    }

    private static synchronized void createTables(FileSystem fs, SQLContext sqlContext) throws Exception {
        String file = "./src/main/resources/createTables.sql";
        String sqls = new String(FSUtils.readDataFile(fs, file));
        sqls = sqls.replace("/user/noce/DATA/PUBLIC/NOCE/", Config.getHdfsRootPath());
        String[] a = sqls.split(";");
        DataFrame df = sqlContext.sql("show tables");
        long count = df.count();
        log.info("tables count=" + count);
        if (count != a.length) {
            log.info("create tables...");
            for (String sql : sqls.split(";")) {
                if (sql == null)
                    continue;
                sql = sql.trim();
                if (sql.isEmpty())
                    continue;
                log.info("sql=" + sql);
                sqlContext.sql(sql);
            }
        }
    }

}
