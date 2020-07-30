package com.work.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Author：黄世良
 * Date：2018/12/16 21:46
 * Description：当用户写程序调用HDFS的API时，NameNode并不会把删除的文件或目录放入回收站Trash中，而是需要自己实现相关的回收站逻辑
 *              代码参考：https://my.oschina.net/cloudcoder/blog/179381
 *                       https://blog.csdn.net/silentwolfyh/article/details/53907118
 *              建议阅读org.apache.hadoop.fs.Trash源码，实际上底层就是rename重命名再加上一些严密的逻辑控制处理代码
 */
public class RemoveFileUtil {

    private final static Logger log = MiscUtils.getLogger(RemoveFileUtil.class);
    private final static Configuration conf = new Configuration();

    /**
     * Delete a file/directory on hdfs
     *
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    public static boolean rm(FileSystem fs, Path path, boolean recursive)
            throws IOException {
        log.info("rm: " + path + " recursive: " + recursive);
        boolean ret = fs.delete(path, recursive);
        if (ret)
            log.info("rm: " + path);
        return ret;

    }

    /**
     * Delete a file/directory on hdfs,and move a file/directory to Trash
     * @param fs
     * @param path
     * @param recursive
     * @param moveToTrash
     * @return
     * @throws IOException
     */
    public static boolean rm(FileSystem fs, Path path, boolean recursive,
                             boolean moveToTrash) throws IOException {
        log.info("rm: " + path + " ,recursive: " + recursive+" ,moveToTrash:"+moveToTrash);
        if (moveToTrash) {
            Trash trashTmp = new Trash(fs, conf);
            //trashTmp.moveToTrash这一行代码如果是在集群，会打印=>例如：fs.TrashPolicyDefault: Moved: '/user/xiangdy/Test' to trash at: viewfs://cluster14/user/xiangdy/.Trash/Current/user/xiangdy/Test1544973745300
            // trashTmp.moveToTrash这一行代码如果是在本地测试，会打印=>例如：fs.TrashPolicyDefault: Namenode trash configuration: Deletion interval = 1440 minutes, Emptier interval = 0 minutes.，注意：这里没有打印删除的目录以及删除后保存到回收站.Trash的目录
            if (trashTmp.moveToTrash(path)) {
                if(fs.getUri().toString().contains("file:///"))
                {
                    //如果在本地测试，则打印删除的目录以及删除后保存到回收站.Trash的目录
                    log.info("Moved '"+path+"' to trash: " + fs.getHomeDirectory()+".Trash/Current/"+path+"  如果该目录在回收站已存在，则在目录名后加上时间戳");
                }
                return true;
            }
        }
        boolean ret = fs.delete(path, recursive);
        if (ret)
            log.info("rm: " + path);
        return ret;

    }

    /***
     * 通过调用cmd的shell来执行hdfs dfs -rm -r xxx 命令，建议使用上一种方式
     *
     * 使用这种方式主要是为了避免调用上一种方式中的moveToTrash方法时报错=》Caused by: java.io.IOException: Renames across Mount points not supported
     * 因为moveToTrash方法底层是rename重命名，然而重命名是不可以在不同挂载点之间进行
     *
     * 但调用hdfs dfs -rm -r 却可以直接moveToTrash
     * @param hdfsPath
     * @return
     */
    public static void rmByExecCmd(String hdfsPath){
        String hdfsCommand="hdfs dfs -rm -r "+hdfsPath;
        //执行删除命令
        exec(hdfsCommand);
    }


    /**
     *   调用shell执行命令
     * @param cmd
     * @return
     */
    public static String exec(String cmd) {
        try {
            String[] cmdA = { "/bin/sh", "-c", cmd };
            Process process = Runtime.getRuntime().exec(cmdA);
            process.waitFor();
            // 获取命令执行结果, 有两个结果: 正常的输出 和 错误的输出（PS: 子进程的输出就是主进程的输入）
            BufferedReader bufrIn =  new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
            BufferedReader bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));
            // 读取输出
            String line = null;
            StringBuffer sb = new StringBuffer();
            sb.append("\n");//先换个行
            while ((line = bufrIn.readLine()) != null) {
                sb.append(line).append('\n');
            }
            while ((line = bufrError.readLine()) != null) {
                sb.append(line).append('\n');
            }

            String result = sb.toString();
            if (!cmd.isEmpty())
                log.info("cmd executed, result: " + result);
            return result;
        } catch (Exception e) {
            log.error("Failed to exec cmd: " + cmd, e);
        }
        return null;
    }


}
