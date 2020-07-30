package com.conf.parse.common;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.conf.parse.config.Config;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiscUtils {

    static {
        initLog4j();
    }

    private static boolean log4jInited;

    // 调用这个方法主要是为避免跟spark或hadoop的日志混杂在一起或受它们影响
    public static synchronized void initLog4j() {
        if (log4jInited)
            return;
        log4jInited = true;
        String log4jFile = System.getProperty(Config.PROJECT_NAME + "_log4j");
        InputStream in = null;
        if (log4jFile != null) {
            try {
                in = new FileInputStream(new File(log4jFile));
            } catch (IOException e) {
            }
        }
        // 要加"/"号，否则返回null
        if (in == null) {
            in = Config.class.getResourceAsStream("/" + Config.PROJECT_NAME + "_log4j.properties");
        }

        Properties properties = new Properties();
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + Config.PROJECT_NAME + "_log4j.properties");
        }
        PropertyConfigurator.configure(properties);
        // 兼容老的log4j版本，这个api从1.2.17才有，一些低版本的hadoop/spark用的log4j太低
        // PropertyConfigurator.configure(in);
    }

    public static Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }

    private static final Logger log = getLogger(MiscUtils.class);

    public static final Charset UTF8 = Charset.forName("UTF-8");
    public static final Charset GBK = Charset.forName("GBK");

    public static void closeStreamSilently(Closeable... streams) {
        for (Closeable stream : streams) {
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
    }

    private static final Pattern number = Pattern.compile("^-?[0-9]+(\\.?[0-9]+)?$");

    public static boolean isNumber(String input) {
        return number.matcher(input).matches();
    }

    public static int toInt(String str) {
        if (str == null)
            return 0;
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return 0;
        }
    }

    public static int toInt(String str, int radix) {
        if (str == null)
            return 0;
        try {
            return Integer.parseInt(str, radix);
        } catch (Exception e) {
            return 0;
        }
    }

    public static long toLong(String str) {
        if (str == null)
            return 0;
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return 0;
        }
    }

    public static double toDouble(String str) {
        return Double.parseDouble(str);
    }

    public static void trim(String[] a) {
        for (int i = 0, size = a.length; i < size; i++) {
            a[i] = a[i].trim();
        }
    }

    public static String[] split(String str, String regex) {
        String[] a = str.split(regex, -1);
        for (int i = 0, size = a.length; i < size; i++) {
            a[i] = a[i].trim();
        }
        return a;
    }

    public static String getMatchingPercent(long matchingCount, long totalCount) {
        if (totalCount <= 0)
            return "0.00%";

        return String.format("%#.2f", (matchingCount / (totalCount * 1.0) * 100)) + "%";
    }

    public static void printMemoryUsage(String prefix, Logger log) {
        log.info(prefix + "Heap size: " + prettyPrintMemoryOrDiskSize(Runtime.getRuntime().freeMemory()) + "/"
                + prettyPrintMemoryOrDiskSize(Runtime.getRuntime().totalMemory()) + "/"
                + prettyPrintMemoryOrDiskSize(Runtime.getRuntime().maxMemory()));

        MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        MemoryUsage nmu = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        log.info("MemoryUsage: " + mu);
        log.info("NonHeapMemoryUsage: " + nmu);
    }

    public static String prettyPrintMemoryOrDiskSize(long size) {
        if (size < 1014)
            return size + "Bytes";
        if (size >= 1 << 30)
            return String.format("%.3fGiB", size / (double) (1 << 30));
        if (size >= 1 << 20)
            return String.format("%.3fMiB", size / (double) (1 << 20));
        return String.format("%.3fKiB", size / (double) (1 << 10));
    }

    public static String currentDate() {
        return currentDate(DateTimeUtils.dateFormat);
    }

    public static String currentDate(SimpleDateFormat dateFormat) {
        synchronized (dateFormat) {
            return dateFormat.format(new Date());
        }
    }

    public static String currentDateTime() {
        synchronized (DateTimeUtils.dateTimeFormat) {
            return DateTimeUtils.dateTimeFormat.format(new Date());
        }
    }

    public static String lastDate() {
        return lastDate(DateTimeUtils.dateFormat);
    }

    public static String lastDate(SimpleDateFormat dateFormat) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        synchronized (dateFormat) {
            return dateFormat.format(calendar.getTime());
        }
    }

    public static String getDate(String dateTime) {
        return dateTime.substring(0, dateTime.length() - 2);
    }

    public static List<String> getDates(String startDate, String endDate) throws Exception {
        return getDates(startDate, endDate, DateTimeUtils.dateFormat);
    }

    public static List<String> getDates(String startDate, String endDate, SimpleDateFormat dateFormat)
            throws Exception {
        List<String> dates = new ArrayList<>();
        Date start = DateTimeUtils.parseDate(startDate, dateFormat);
        Date end = DateTimeUtils.parseDate(endDate, dateFormat);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        while (calendar.getTime().compareTo(end) <= 0) {
            dates.add(DateTimeUtils.formatDate(calendar.getTime(), dateFormat));
            calendar.add(Calendar.DAY_OF_MONTH, 1);
        }

        return dates;
    }

    public static String appendSlash(String str) {
        if (str == null)
            return null;
        if (!str.endsWith("/"))
            str = str + "/";
        return str;
    }

    public static String prependSlash(String str) {
        if (str == null)
            return null;
        if (!str.startsWith("/"))
            str = "/" + str;
        return str;
    }

    public static String getPercent(double p) {
        return getPercent(p, false);
    }

    public static String getPercent(double p, boolean appendPercentChar) {
        String percentChar = "";
        if (appendPercentChar)
            percentChar = "%";
        if (p <= 0)
            return "0.00" + percentChar;

        return String.format("%#.2f", p * 100) + percentChar;
    }

    public static String getPercent(long count, long totalCount) {
        return getPercent(count, totalCount, false);
    }

    public static String getPercent(long count, long totalCount, boolean appendPercentChar) {
        String percentChar = "";
        if (appendPercentChar)
            percentChar = "%";

        if (totalCount <= 0)
            return "0.00" + percentChar;

        return String.format("%#.2f", (count / (totalCount * 1.0) * 100)) + percentChar;
    }

    private static ThreadPoolExecutor threadPoolExecutor;

    public static synchronized ThreadPoolExecutor getThreadPoolExecutor() {
        return getThreadPoolExecutor(-1);
    }

    public static synchronized ThreadPoolExecutor getThreadPoolExecutor(int maximumPoolSize) {
        if (threadPoolExecutor == null) {
            threadPoolExecutor = createThreadPoolExecutor(maximumPoolSize);
        }
        return threadPoolExecutor;
    }

    public static ThreadPoolExecutor createThreadPoolExecutor(int maximumPoolSize) {
        if (maximumPoolSize <= 0)
            maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedDeamonThreadFactory());
        tpe.setRejectedExecutionHandler(blockingExecutionHandler);
        tpe.allowsCoreThreadTimeOut();
        return tpe;
    }

    public static ThreadPoolExecutor createThreadPoolExecutor(String name, int maximumPoolSize) {
        if (maximumPoolSize <= 0)
            maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedDeamonThreadFactory(name));
        tpe.setRejectedExecutionHandler(blockingExecutionHandler);
        tpe.allowsCoreThreadTimeOut();
        return tpe;
    }

    public static ThreadPoolExecutor createSynchronousThreadPoolExecutor(int maximumPoolSize) {
        if (maximumPoolSize <= 0)
            maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedDeamonThreadFactory());
        tpe.setRejectedExecutionHandler(blockingExecutionHandler);
        tpe.allowsCoreThreadTimeOut();
        return tpe;
    }

    public static ThreadPoolExecutor createSynchronousThreadPoolExecutor(String name, int maximumPoolSize) {
        if (maximumPoolSize <= 0)
            maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedDeamonThreadFactory(name));
        tpe.setRejectedExecutionHandler(blockingExecutionHandler);
        tpe.allowsCoreThreadTimeOut();
        return tpe;
    }

    public static VolatileExecutor createVolatileExecutor(String name) {
        return createVolatileExecutor(name, -1);
    }

    public static VolatileExecutor createVolatileExecutor(String name, int maximumPoolSize) {
        if (maximumPoolSize <= 0)
            maximumPoolSize = Runtime.getRuntime().availableProcessors();
        return new VolatileExecutor(1, maximumPoolSize, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                new NamedDeamonThreadFactory(name));
    }

    public static class VolatileExecutor implements AutoCloseable {
        private final ArrayList<Future<?>> futures = new ArrayList<>();
        private final ArrayList<Object> tasks = new ArrayList<>();
        private final ThreadPoolExecutor threadPoolExecutor;

        public VolatileExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                    threadFactory);
            threadPoolExecutor.setRejectedExecutionHandler(blockingExecutionHandler);
            threadPoolExecutor.allowsCoreThreadTimeOut();
        }

        @Override
        public void close() throws Exception {
            if (!futures.isEmpty()) {
                await();
            }
            threadPoolExecutor.shutdown();
        }

        public void submitTask(Runnable task) {
            try {
                futures.add(threadPoolExecutor.submit(task));
                tasks.add(task);
            } catch (Exception e) {
                log.error("Failed to submit task: " + task, e);
            }
        }

        public void submitTasks(List<?> tasks) {
            for (Object task : tasks) {
                if (task instanceof Runnable)
                    submitTask((Runnable) task);
                else if (task instanceof Callable) {
                    submitTask((Callable<?>) task);
                } else {
                    log.warn("Invalid task: " + task);
                }
            }
        }

        public void submitTask(Callable<?> task) {
            try {
                futures.add(threadPoolExecutor.submit(task));
                tasks.add(task);
            } catch (Exception e) {
                log.error("Failed to submit task: " + task, e);
            }
        }

        public void await() {
            for (int i = 0, size = tasks.size(); i < size; i++) {
                try {
                    futures.get(i).get();
                } catch (Exception e) {
                    log.error("Task exception: " + tasks.get(i), e);
                }
            }
            futures.clear();
            tasks.clear();
        }
    }

    public static class NamedDeamonThreadFactory implements ThreadFactory {
        private final String id;
        private final int priority;
        private final AtomicInteger n = new AtomicInteger(1);

        public NamedDeamonThreadFactory() {
            this.id = Config.PROJECT_NAME + "-thread";
            this.priority = Thread.NORM_PRIORITY;
        }

        public NamedDeamonThreadFactory(String id) {
            this(id, Thread.NORM_PRIORITY);
        }

        public NamedDeamonThreadFactory(String id, int priority) {
            this.id = Config.PROJECT_NAME + "-" + id + "-thread";
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            String name = id + "-" + n.getAndIncrement();
            Thread thread = new Thread(runnable, name);
            thread.setPriority(priority);
            thread.setDaemon(true);
            return thread;
        }
    }

    public static final RejectedExecutionHandler blockingExecutionHandler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (true) {
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
                }
                try {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        }
    };
}
