package com.conf.parse.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import com.conf.parse.common.MiscUtils;
import org.slf4j.Logger;

public class Config {


    private static final Logger log = MiscUtils.getLogger(Config.class);


    public static void main(String[] args) {
        Config.dev_test_mode = true;
        System.out.println(getHdfsRootPath());
    }

    public static final String PROJECT_NAME = "conf_parse";
    public static final String VERSION = "1.0.0";
    public static String configPath;
    private static final Properties config = loadConfig();
    //private static final Properties outerConfig = loadOuterConfig();
    public static boolean dev_test_mode;


    private static String hdfs_root_path;

    // 已经在后面加了"/"
    public static String getHdfsRootPath() {
        if (hdfs_root_path == null) {
            hdfs_root_path = getPath("hdfs_root_qyztyh_path");
            // 创建分区时不能在location那里指定一个相对路径，所以不能直接用原始的hdfs_root_path配置
            if (dev_test_mode) {
                try {
                    hdfs_root_path = "file:///" + new File(hdfs_root_path).getCanonicalPath().replace('\\', '/');
                } catch (IOException e) {
                    hdfs_root_path = "file:///" + new File(hdfs_root_path).getAbsolutePath().replace('\\', '/');
                }
            }
        }
        hdfs_root_path = MiscUtils.appendSlash(hdfs_root_path);
        return hdfs_root_path;
    }

    // HDFS 相关的路径参数
    // ------------------------------------------------------------
    public static final String pm_etl_path = getQyztyhPath("pm_etl_path");
    public static final String pm_agg_path = getQyztyhPath("pm_agg_path");

    //src
    public static final String conf_parse_src_hw_path = getQyztyhPath("conf_parse_src_hw_path");
    public static final String conf_parse_src_zte_path = getQyztyhPath("conf_parse_src_zte_path");
    public static final String conf_parse_src_ericsson_path = getQyztyhPath("conf_parse_src_ericsson_path");

    //zte
    public static final String conf_parse_etl_base_enodeb = getQyztyhPath("conf_parse_etl_base_enodeb");
    public static final String conf_parse_etl_zte_localcell = getQyztyhPath("conf_parse_etl_zte_localcell");
    public static final String conf_parse_etl_zte_adjacencycell = getQyztyhPath("conf_parse_etl_zte_adjacencycell");
    public static final String conf_parse_etl_zte_neighborrelation = getQyztyhPath("conf_parse_etl_zte_neighborrelation");
    public static final String conf_parse_etl_zte_plmn = getQyztyhPath("conf_parse_etl_zte_plmn");

    //hw
    public static final String conf_parse_etl_hw_bts3900cellop = getQyztyhPath("conf_parse_etl_hw_bts3900cellop");
    public static final String conf_parse_etl_hw_bts3900cnoperator = getQyztyhPath("conf_parse_etl_hw_bts3900cnoperator");
    public static final String conf_parse_etl_hw_bts3900cnoperatorta = getQyztyhPath("conf_parse_etl_hw_bts3900cnoperatorta");
    public static final String conf_parse_etl_hw_bts3900eutranexternalcell = getQyztyhPath("conf_parse_etl_hw_bts3900eutranexternalcell");
    public static final String conf_parse_etl_hw_localcell = getQyztyhPath("conf_parse_etl_hw_localcell");

    //所有厂家
    public static final String conf_parse_etl_function = getQyztyhPath("conf_parse_etl_function");
    public static final String conf_parse_etl_localcell = getQyztyhPath("conf_parse_etl_localcell");
    public static final String conf_parse_etl_adjacencycell = getQyztyhPath("conf_parse_etl_adjacencycell");
    public static final String conf_parse_etl_neighborrelation = getQyztyhPath("conf_parse_etl_neighborrelation");
    public static final String conf_parse_agg_lnnrwhole = getQyztyhPath("conf_parse_agg_lnnrwhole");

    public static final String conf_parse_agg_pcifault = getQyztyhPath("conf_parse_agg_pcifault");
    public static final String conf_parse_agg_argsfault = getQyztyhPath("conf_parse_agg_argsfault");



    public static final String hive_db_name = getStringProperty("hive_db_name");

    //读取jar包外配置文件
    private static Properties loadOuterConfig() {
        if ((Options.isAnalysisType || Options.analysisTypeOrderArray == null) && !Options.isEtlType)//主要为了Config判断如果是跑AGG就不用读外部配置文件
        {
            log.warn("Config类的loadOuterConfig方法返回null，不读取外部配置文件");
            return null;
        }
        InputStream in = null;
        if (in == null) {
            log.warn("外部配置文件绝对路径：" + Options.configPath);
            try {
                in = new FileInputStream(Options.configPath);
            } catch (Exception e) {
                log.error("外部配置文件绝对路径：" + Options.configPath + "  找不到,spark-submit时要加上-configPath参数");
                log.error(e.getMessage());
                Options.printUsage();
            }
        }
        Properties config = new Properties();
        try {
            config.load(new InputStreamReader(in, "UTF-8"));   //不用下面那种方式，是为了防止配置文件中的中文乱码
//	      config.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + PROJECT_NAME + "_conf.properties");
        }
        return config;
    }


    //读取jar包内配置文件
    private static Properties loadConfig() {
        InputStream in = null;
        if (in == null) {
            in = Config.class.getResourceAsStream("/" + PROJECT_NAME + "_conf.properties");
        }
        Properties config = new Properties();
        try {
            config.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + PROJECT_NAME + "_conf.properties");
        }
        return config;
    }

    public static int getIntProperty(String key) {
        String v = config.getProperty(key);
        if (v == null)
            return -1;
        v = v.trim();
        return Integer.parseInt(v);
    }

    public static String getStringProperty(String key) {
        String v = config.getProperty(key);
        if (v == null)
            return null;
        v = v.trim();
        return v;
    }

    public static boolean getBooleanProperty(String key) {
        String v = config.getProperty(key);
        if (v == null)
            return false;
        v = v.trim();
        return Boolean.parseBoolean(v);
    }

    public static String getPath(String key) {
        String path = getStringProperty(key);
        path = MiscUtils.appendSlash(path);
        return path;
    }

    //QYZTY根目录
    private static String getQyztyhPath(String key) {
        String hdfs_root_qyztyh_path = getPath("hdfs_root_qyztyh_path");
        String path = getPath(key);
        return hdfs_root_qyztyh_path + path;
    }


}
