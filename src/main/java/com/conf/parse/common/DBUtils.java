/** 
 * 项目名称:qyztyh 
 * 文件名称:DBUtils.java 
 * 包名:com.wellcommsoft.qyztyh.common 
 * 创建时间:2017年12月14日下午5:11:21 
 * Copyright (c) 2017, liyonghong@wellcommsoft.com All Rights Reserved. 
 * 
 */
package com.conf.parse.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import com.conf.parse.config.Config;

/**
 * 操作数据库工具类
 *
 *
 */
public class DBUtils {

    /**
     * 连接数据
     *
     * @return conn
     */
    public static Connection getConnection() {
        Connection conn = null;
        Properties config = loadConfig();
        try {
            Class.forName(config.getProperty("driver"));
            conn = DriverManager.getConnection(config.getProperty("url"), config.getProperty("username"),
                    config.getProperty("password"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接对象
     *
     * @param conn
     *            连接对象
     * @param pstmt
     *            预编译对象
     * @param rs
     *            结果集
     */
    public static void closeAll(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 增删改操作
     *
     * @param sql
     *            SQL命令
     * @param param
     *            参数
     * @return
     */
    public static int executUpdate(Connection conn, String sql, Object[] param) {
        int result = 0;
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    pstmt.setObject(i + 1, param[i]);
                }
            }
            result = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAll(conn, pstmt, null);
        }
        return result;
    }

    /**
     * 查询
     *
     * @return int
     */
    public static ResultSet executQuery(Connection conn, String sql, String[] param) {
        PreparedStatement pstmt = null;
        ResultSet result = null;
        try {
            pstmt = conn.prepareStatement(sql);
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    pstmt.setString(i + 1, param[i]);
                }
            }
            result = pstmt.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static Properties loadConfig() {
        String confFile = System.getProperty("DB_conf");
        InputStream in = null;
        if (confFile != null) {
            try {
                in = new FileInputStream(new File(confFile));
            } catch (IOException e) {
            }
        }
        // 要加"/"号，否则返回null
        if (in == null) {
            in = Config.class.getResourceAsStream("/DB_conf.properties");
        }
        Properties config = new Properties();
        try {
            config.load(in);
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load DB_conf.properties");
        }
        return config;
    }
}
