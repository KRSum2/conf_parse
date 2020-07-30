package com.work.etl;

import com.conf.parse.common.MiscUtils;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;

public abstract class ETL {

    private static final Logger log = MiscUtils.getLogger(ETL.class);

    public void run(FileSystem fs, HiveContext sqlContext, Options options, JavaSparkContext jsc) throws Exception {
        TaskStartTime tst = new TaskStartTime(options.dateTime);
        String srcPath = getSrcPath(fs, options);
         run(fs, sqlContext, srcPath, Config.pm_etl_path, tst, jsc);



    }

    /**
     * 获取源文件，每个处理类继承ETL时，实现该方法，返回路径Config.xxxx
     * @param fs
     * @param options
     * @return
     */
    public abstract String getSrcPath(FileSystem fs, Options options);

    /**
     * 运行分析程序
     *
     * @param fs
     * @param savePath   保存路径，已经加了"/"后缀，但是不包含表名
     * @param tst        任务开始时间，只到小时
     * @throws Exception
     */
    public abstract void run(FileSystem fs,  HiveContext sqlContext, String srcPath, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception;

/*    public static void printResult(DataFrame df) {
        for (String c : df.columns()) {
            log.info("column name: " + c);
        }

        for (Row row : df.collect()) {
            log.info("row: " + row);
            // for (int i = 0, len = row.length(); i < len; i++) {
            // log.info("col" + (i + 1) + ": " + row.getString(i)); //获取第1列，下标是0不是1
            // }
        }
    }*/

    public static void createPartition(SQLContext sqlContext, String tableName, String partitionName,
            String partitionValue, String locationInfix) {
        locationInfix = MiscUtils.appendSlash(locationInfix);
        String sql = "alter table " + tableName + " add IF NOT EXISTS partition (" + partitionName + "='"
                + partitionValue + "') location '" + Config.getHdfsRootPath() + locationInfix + tableName + "/"
                + partitionValue + "'";
        log.info("create partition: " + sql);
        sqlContext.sql(sql);
    }
}
