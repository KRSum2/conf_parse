package com.work.analysis;

import com.conf.parse.common.MiscUtils;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import com.work.common.MiscUtils;
import com.work.common.TaskStartTime;
import com.work.config.Config;
import com.work.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;

public abstract class Analysis {

    private static final Logger log = MiscUtils.getLogger(Analysis.class);

    public void run(FileSystem fs, HiveContext sqlContext, Options options, JavaSparkContext jsc) throws Exception {
        TaskStartTime tst = new TaskStartTime(options.dateTime);
        run(fs, sqlContext, Config.pm_agg_path, tst, jsc);
    }
    /**
     * 运行分析程序
     *
     * @param fs
     * @param savePath   保存路径，已经加了"/"后缀，但是不包含表名
     * @param tst        任务开始时间，只到小时
     * @throws Exception
     */
    public abstract void run(FileSystem fs,  HiveContext sqlContext, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception;

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
