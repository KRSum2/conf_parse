package com.conf.parse.main;

import com.conf.parse.analysis.AnalysisType;
import com.conf.parse.common.FSUtils;
import com.conf.parse.common.MiscUtils;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;

public class DataAnalysis {

  private static final Logger log = MiscUtils.getLogger(DataAnalysis.class);
  private static final String CLASS_NAME = DataAnalysis.class.getSimpleName();

  public static void main(String[] args) {
    //设置使用旧版本的JDK 比较器函数内部实现
    FileSystem fs = null;
    JavaSparkContext jsc = null;
    try {
      fs = FSUtils.getNewFileSystem();
      Options options = Options.parseArgs(fs, args);
      String appName = CLASS_NAME + "_" + options.dateTimes;
      appName += "_" + options.analysisTypes;
      SparkConf sparkConf = options.getSparkConf(appName);   //默认为集群模式(这里的参数可以调整)
      sparkConf.set("spark.executor.memory","24g")
              .set("spark.driver.maxResultSize", "24g")
              .set("spark.kryoserializer.buffer","1024m")
              .set("spark.kryoserializer.buffer.max", "1024m");

      jsc = new JavaSparkContext(sparkConf);
      // 启动Analysis任务
      // 在Spark 1.6中这个SQLContext还不能使用hive中的表
      //SQLContext sqlContext = new SQLContext(jsc);
      HiveContext sqlContext = new HiveContext(jsc);

      // 生成HiveContext后，必需指定hive的数据库名
      if (!options.devTestMode)
        sqlContext.sql("use " + Config.hive_db_name);

      //是否按照指定的先后顺序跑
      if (!options.runByOrder) {
        startAnalysisTasks(fs, sqlContext, options, jsc);
      } else {
        startAnalysisTasksByOrder(fs, sqlContext, options, jsc);
      }
    } catch (Throwable t) {
      log.error(CLASS_NAME + ".startAnalysisTasks exception", t);
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

  private static void startAnalysisTasks(final FileSystem fs, final HiveContext sqlContext, final Options options, final JavaSparkContext jsc)
          throws Exception {
    try (MiscUtils.VolatileExecutor executor = MiscUtils.createVolatileExecutor("com/conf_parse/analysis")) {
      for (final AnalysisType analysisType : options.analysisTypes) {
        executor.submitTask(new Runnable() {
          @Override public void run() {
            try {
              if (analysisType.runByDay()) {
                for (String date : options.getDates()) {
                  options.dateTime = date + "00";
                  analysisType.newAnalysisInstance().run(fs, sqlContext, options, jsc);
                }

              } else {
                for (String dateTime : options.dateTimes) {
                  options.dateTime = dateTime;
                  analysisType.newAnalysisInstance().run(fs, sqlContext, options, jsc);
                }

              }
            } catch (Exception e) {
              log.error("Failed to run analysis task: " + analysisType, e);
            }
          }
        });
      }
    }
  }

  private static void startAnalysisTasksByOrder(final FileSystem fs, final HiveContext sqlContext,
          final Options options, final JavaSparkContext jsc) throws Exception {
    for (final AnalysisType analysisType : options.analysisTypeOrderArray) {
      log.info("执行：" + analysisType + " : " + System.currentTimeMillis());
      try {
        if (analysisType.runByDay()) {
          for (String date : options.getDates()) {
            options.dateTime = date + "00";
            analysisType.newAnalysisInstance().run(fs, sqlContext, options, jsc);
          }

        } else {
          for (String dateTime : options.dateTimes) {
            options.dateTime = dateTime;
            analysisType.newAnalysisInstance().run(fs, sqlContext, options, jsc);
          }
        }
      } catch (Exception e) {
        log.error("Failed to run analysis task: " + analysisType, e);
      }
    }
  }
}
