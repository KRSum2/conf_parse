package com.conf.parse.config;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;

import com.conf.parse.analysis.AnalysisType;
import com.conf.parse.common.DateTimeUtils;
import com.conf.parse.common.MiscUtils;
import com.conf.parse.etl.ETLType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;

public class Options implements Serializable {

    private static final Logger log = MiscUtils.getLogger(Options.class);

    public long maxBatchFilesSize = 100L * 1024 * 1024; // 默认是100M

    public boolean devTestMode = false;
    boolean useKryoSerializer = false;
    public boolean lockEnabled = false;

    // public boolean addHivePartitions = true;
    public boolean mergePartitionsOnExit = true;
    public boolean deletePartitionsOnExit = true;
    public boolean deleteSrcFilesOnEtled = false;
    public boolean updateEtlMetaDataFile = true; //更新_MD_文件夹，由于权限问题，无法写，加个参数控制下，免得重新发布

    public boolean daemon = false;
    
    public static  String configPath =null;  //外部配置文件路径
    public static Boolean isAnalysisType=false;  //主要为了Config判断如果是跑AGG就不用读外部配置文件
    public static Boolean isEtlType = false;	//主要为了Config判断如果是跑AGG就不用读外部配置文件
    
    public ArrayList<String> dateTimes = new ArrayList<>();
    public String dateTime; // 例如: "2016050623"，第一个时间（即开始时间）
    public String[] allCityArrays = {"GZ", "SZ", "ZH", "ST", "FS", "SG", " ZJ", "ZQ", "JM", "MM", "HZ", "MZ", "SW", "HY", "YJ", "QY", "DG", "ZS", "CZ", "JY", "YF"};
    public String[] citys;
    public String city;     // 例如："GZ"
    public boolean warnNotExistsDateTime = true;
    
    public final HashSet<ETLType> etlTypes = new HashSet<>();
    public final HashSet<AnalysisType> analysisTypes = new HashSet<>();
    public static ETLType[] etlTypeOrderArray = null;
    public static AnalysisType[] analysisTypeOrderArray = null;
    
    public int checkInterval = 2 * 60 * 1000; // 两分钟检查一次
    public String[] args;
    public boolean rerun;
    public boolean runByOrder = false; //按指定的参数顺序跑,默认false

    public Options newInstance(FileSystem fs, String dateTime) throws Exception {
        String[] newArgs = new String[args.length];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        for (int i = 0, size = newArgs.length; i < size; i++) {
            if (newArgs[i].equals("-dateTime")) {
                newArgs[i + 1] = dateTime;
            }
        }

        Options options = new Options();
        options.parse(newArgs);
        options.init(fs);
        return options;
    }

    private void parseDateTime(String dateTime) throws Exception {
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DateTimeUtils.DATE_TIME_FORMAT);
        int pos = dateTime.indexOf('-');
        if (pos > 0) {
            String startDateTime = dateTime.substring(0, pos);
            String endDateTime = dateTime.substring(pos + 1);

            long startT = dateTimeFormat.parse(startDateTime).getTime();
            long endT = dateTimeFormat.parse(endDateTime).getTime();
            long hour = 60 * 60 * 1000; // 一小时
            for (long i = startT; i <= endT; i += hour) {
                dateTimes.add(dateTimeFormat.format(new Date(i)));
            }
        } else {
            String[] dateTimeArray = dateTime.split(",");
            for (String dt : dateTimeArray) {
                dateTimes.add(dt);
            }
        }

        this.dateTime = dateTimes.get(0);
    }

    private void parseCity(String cityStr) {
        try{
            citys = cityStr.split(",");
            if (citys.length == 1){
                if (citys[0].equals("QS") || citys[0].equals("qs")) {         //全省
                    citys = allCityArrays;                                                     //将所有地市全部加入
                }
            }
        }catch (Exception e){
            log.error("city参数有异常：" + e);
        }
    }



    private void parse(String[] args) {
        this.args = args;
        info("开始解析选项参数...");

        if (args == null || args.length == 0)
            printErrorAndExit("未指定任何参数");

        if (log.isDebugEnabled()) {
            if (args.length > 2) {
                for (int i = 0; i < args.length; i++) {
                    log.debug(args[i]);
                }

                for (ETLType t : etlTypes) {
                    log.debug(t.toString());
                }
            }
        }

        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                error("选项参数不能包含null");
                printUsageAndExit();
            }
            args[i] = args[i].trim();
        }
        ETLType[] etlTypes = ETLType.values();                      //获取所有ETL枚举类型,返回括号前面的类型
        AnalysisType[] analysisTypes = AnalysisType.values();       //获取所有AGG枚举类型
        try {
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                switch (a) {
                case "-daemon":
                    daemon = Boolean.parseBoolean(args[++i]);
                    break;
                case "-maxBatchFilesSizeMB":
                    maxBatchFilesSize = 1L * Long.parseLong(args[++i]) * 1024 * 1024;
                    if (maxBatchFilesSize <= 0) {
                        error("-maxBatchFilesSizeMB 必须指定一个大于或等于1的参数"); 
                    }
                    break;
                case "-etlTypes":
                	isEtlType=true;
                    String[] etlTypeArray = args[++i].split(",");
                    etlTypeOrderArray = etlTypes = new ETLType[etlTypeArray.length];
                    for (int j = 0; j < etlTypeArray.length; j++) {
                        etlTypeOrderArray[j] = etlTypes[j] = ETLType.valueOf(etlTypeArray[j]);     //返回etlTypeArray[j]的类型
                    }
                    break;
                case "-analysisTypes":
                	isAnalysisType=true; //主要为了Config判断如果是跑AGG就不用读外部配置文件
                    String[] analysisTypeArray = args[++i].split(",");
                    analysisTypeOrderArray = analysisTypes = new AnalysisType[analysisTypeArray.length];
                    for (int j = 0; j < analysisTypeArray.length; j++) {
                        analysisTypeOrderArray[j] = analysisTypes[j] = AnalysisType.valueOf(analysisTypeArray[j]);
                    }
                    break;

                case "-dateTime":
                    parseDateTime(args[++i]);
                    break;
                //增加一个地市参数
                case "-city":
                    parseCity(args[++i]);
                    break;
                case "-configPath":
                	configPath=args[++i];  //外部配置文件路径
                	break;
                case "-mergePartitionsOnExit":
                    mergePartitionsOnExit = Boolean.parseBoolean(args[++i]);
                    break;
                case "-deletePartitionsOnExit":
                    deletePartitionsOnExit = Boolean.parseBoolean(args[++i]);
                    break;
                case "-deleteSrcFilesOnEtled":
                    deleteSrcFilesOnEtled = Boolean.parseBoolean(args[++i]);
                    break;
                case "-updateEtlMetaDataFile":
                    updateEtlMetaDataFile = Boolean.parseBoolean(args[++i]);
                    break;
                case "-help":
                    printUsageAndExit();
                    break;
                case "-devTestMode":
                    devTestMode = Boolean.parseBoolean(args[++i]);
//                    Config.dev_test_mode = devTestMode; //不需要了
                    break;
                case "-useKryoSerializer":
                    useKryoSerializer = Boolean.parseBoolean(args[++i]);
                    break;
                case "-lockEnabled":
                    lockEnabled = Boolean.parseBoolean(args[++i]);
                    break;
                case "-warnNotExistsDateTime":
                    warnNotExistsDateTime = Boolean.parseBoolean(args[++i]);
                    break;
                case "-startTime":
                    i++;
                    break;
                case "-checkInterval":
                    checkInterval = Integer.parseInt(args[++i]) * 1000;
                    break;
                case "-delayHours":
                    i++;
                    break;
                case "-runByOrder":
                    runByOrder = Boolean.parseBoolean(args[++i]);
                    break;
                case "-rerun":
                    rerun = Boolean.parseBoolean(args[++i]);
                    break;
                default:
                    error("选项名 '" + a + "' 无效");
                    printUsageAndExit();
                }
            }
        } catch (Exception e) {
            error("参数解析错误", e);
            printUsageAndExit();
        }
        this.etlTypes.addAll(Arrays.asList(etlTypes));
        this.analysisTypes.addAll(Arrays.asList(analysisTypes));

    }



    private void initDateTime() throws Exception {
        if (dateTime == null) {
            error("必须指定-dateTime选项");
            printUsageAndExit();
        }
        if (dateTime.length() < 10) {
            error("-dateTime指定的日期时间格式不正确, 正确格式为YYYYMMDDHH");
            printUsageAndExit();
        }
        info("dateTime = " + dateTime);
    }

    private void init(FileSystem fs) throws Exception {
        info("正在初始化...");
        initDateTime();
    }

    public static void printUsage() {
        System.out.println();
        System.out.println("用法: spark-submit --class <main class name>"
                + " --master yarn-client --jars ./xxx.jar [其他spark-submit专用参数] " + Config.PROJECT_NAME + "-"
                + Config.VERSION + ".jar [选项]");
        System.out.println();
        System.out.println("支持以下选项：");

        println("-help", "打印帮助信息");
        println("-dateTime 日期和时间", "需要关联的mr的日期和时间，格式为YYYYMMDDHH (**必选项**)");
        println("-city 地市", "格式为： 例如：GZ (**必选项**)");
        println("-configPath 外部配置文件绝对路径(如：/user/**/**/**)",  "(**必选项**)");
      
        println("-devTestMode true|false", "是否使用local的方式启动spark-submit，仅用于开发测试模式(默认是false)");
        println("-useKryoSerializer true|false", "是否使用KryoSerializer(默认是false)");
        println("-warnNotExistsDateTime true|false", "如果找不到与日期时间对应的目录，是否输出警告(默认是true)");
    }

    private static void println(String s1, String s2) {
        System.out.println("\t" + s1);
        System.out.println("\t\t" + s2);
        System.out.println();
    }

    private static void printErrorAndExit(String format, Object... args) {
        log.error(String.format(format, args));
        printUsage();
        System.exit(-1);
    }

    private static void printUsageAndExit() {
        printUsage();
        System.exit(-1);
    }

    private static void info(String msg) {
        log.info(msg);
    }

    private static void error(String msg) {
        log.error(msg);
    }

    private static void error(String msg, Exception e) {
        log.error(msg, e);
    }

    // 注册要序列化的自定义类型，KryoSerializer要比Java的内置序列化要快
    private static void registerKryoClasses(SparkConf sparkConf) {
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 可以在这里添加要序列化的类
        // sparkConf.registerKryoClasses(new Class<?>[] {});
    }

    public static Options parseArgs(FileSystem fs, String[] args) throws Exception {
        Options options = new Options();
        options.parse(args);
        options.init(fs);
        return options;
    }

    public SparkConf getSparkConf(String appName) {
        SparkConf sparkConf = new SparkConf();
        if (devTestMode)
            sparkConf.setMaster("local[3]");
        // sparkConf.setAppName(appName.toString());
        // sparkConf.set("spark.driver.allowMultipleContexts", "true");
        // sparkConf.set("spark.blockManager.port", "61111");
        if (useKryoSerializer)
            registerKryoClasses(sparkConf);
        if (appName != null)
            sparkConf.setAppName(appName);
        return sparkConf;
    }

    public LinkedHashSet<String> getDates() {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String dateTime : dateTimes) {
            set.add(dateTime.substring(0, 8));
        }
        return set;
    }
}
