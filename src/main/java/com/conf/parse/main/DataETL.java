package com.conf.parse.main;

import com.conf.parse.common.*;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import com.conf.parse.etl.ETLType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;

public class DataETL {

  private static final Logger log = MiscUtils.getLogger(DataETL.class);
  private static final String CLASS_NAME = DataETL.class.getSimpleName();

  public static void main(String[] args) {
    FileSystem fs = null;
    JavaSparkContext jsc = null;
    try {
      fs = FSUtils.getNewFileSystem();
      Options options = Options.parseArgs(fs, args);
      String appName = CLASS_NAME + "_" + options.dateTimes;
      appName += "_" + options.etlTypes;
      SparkConf sparkConf = options.getSparkConf(appName);   //默认为集群模式（下面的参数大小可能会不够）
      //sparkConf.setMaster("local[3]");
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");  //序列化
      sparkConf.set("spark.kryoserializer.buffer.max","256");
      sparkConf.set("spark.kryoserializer.buffer","64");
      sparkConf.set("spark.driver.allowMultipleContexts", "true");    //允许两种sc共存
      jsc = new JavaSparkContext(sparkConf);
      // 启动etl任务
      // 在Spark 1.6中这个SQLContext还不能使用hive中的表
      //SQLContext sqlContext = new SQLContext(jsc);
      HiveContext sqlContext = new HiveContext(jsc);

      // 生成HiveContext后，必需指定hive的数据库名
      if (!options.devTestMode)
        sqlContext.sql("use " + Config.hive_db_name);

      //是否按照指定的先后顺序跑
      if (!options.runByOrder) {
        startEtlTasks(fs, sqlContext, options, jsc);
      } else {
        startEtlTasksByOrder(fs, sqlContext, options, jsc);
      }
    } catch (Throwable t) {
      log.error(CLASS_NAME + ".startEtlTasks exception", t);
    } finally {
      try {
        if (jsc != null)
          jsc.close();
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

  private static void startEtlTasks(final FileSystem fs, final HiveContext sqlContext, final Options options, final JavaSparkContext jsc)
          throws Exception {
    try (MiscUtils.VolatileExecutor executor = MiscUtils.createVolatileExecutor("com/conf_parse/etl")) {
      for (final ETLType etlType : options.etlTypes) {
        executor.submitTask(new Runnable() {
          @Override public void run() {
            try {
              if (etlType.runByDay()) {
                for (String date : options.getDates()) {
                  options.dateTime = date + "00";
                  etlType.newEtlInstance().run(fs, sqlContext, options, jsc);
                }
              } else {
                for (String dateTime : options.dateTimes) {
                  options.dateTime = dateTime;
                  etlType.newEtlInstance().run(fs, sqlContext, options, jsc);
                }
              }
            } catch (Exception e) {
              log.error("Failed to run etl task: " + etlType, e);
            }
          }
        });
      }
    }
  }

  private static void startEtlTasksByOrder(final FileSystem fs, final HiveContext sqlContext,
          final Options options, final JavaSparkContext jsc) throws Exception {
    for (final ETLType etlType : options.etlTypeOrderArray) {
      log.info("执行：" + etlType + " : " + System.currentTimeMillis());
      try {
        if (etlType.runByDay()) {
          for (String date : options.getDates()) {
            options.dateTime = date + "00";
            etlType.newEtlInstance().run(fs, sqlContext, options, jsc);
          }
        } else {
          for (String dateTime : options.dateTimes) {
            options.dateTime = dateTime;
            etlType.newEtlInstance().run(fs, sqlContext, options, jsc);
          }
        }
      } catch (Exception e) {
        log.error("Failed to run etl task: " + etlType, e);
      }
    }
  }
}
