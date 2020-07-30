package com.work.analysis.DataImportMySQL;

import com.conf.parse.analysis.Analysis;
import com.conf.parse.analysis.obj.NrArgsFault;
import com.conf.parse.analysis.obj.NrPciFault;
import com.conf.parse.common.DBUtils;
import com.conf.parse.common.MiscUtils;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.work.analysis.Analysis;
import com.work.analysis.obj.NrPciFault;
import com.work.common.DBUtils;
import com.work.common.MiscUtils;
import com.work.common.TaskStartTime;
import com.work.config.Config;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

public class DataImportAnalysis extends Analysis implements Serializable {

    private static final Logger log = MiscUtils.getLogger(DataImportAnalysis.class);

    @Override
    public void run(FileSystem fs, HiveContext sqlContext, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception {
        Connection connection = DBUtils.getConnection();
        connection.setAutoCommit(false);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        //转换时间格式(作为数据库数据的时间)
        String time = tst.dateStr.substring(0, 4) + "-" + tst.dateStr.substring(4, 6) + "-" + tst.dateStr.substring(6, 8);

        //读取PCI混淆结果
        List<NrPciFault> nrPciFaultList = readConfuse(fs, tst.dateStr, time, jsc);


        //读取PCI参数异常结果
        List<NrArgsFault> nrArgsFaultList = readAgrsEX(fs, tst.dateStr, time,jsc);


        //重跑的时候检查是否出现该天的数据
        delete(connection, time, "agg_nr_pci_fault");
        delete(connection, time, "agg_nr_args_fault");

        //将数据导入数据库
        insertConfuseData(connection, time, nrPciFaultList);
        insertArgsData(connection, time, nrArgsFaultList);


    }



    private List<NrPciFault> readConfuse(FileSystem fs, final String dateStr, final String time, JavaSparkContext jsc) {
        List<NrPciFault> nrPciFaultList = jsc.textFile(Config.conf_parse_agg_pcifault + "/day=" + dateStr).map(new Function<String, NrPciFault>() {
            @Override
            public NrPciFault call(String line) throws Exception {
                String[] lineSplit = line.split(",", -1);
                NrPciFault nrPciFault = new NrPciFault();
                if (lineSplit.length > 22) {
                    nrPciFault.sc_enodebid = lineSplit[0];
                    nrPciFault.sc_enodebname = lineSplit[1];
                    nrPciFault.sc_cellid = NumberUtils.isNumber(lineSplit[2])?Integer.parseInt(lineSplit[2]) : 0;
                    nrPciFault.sc_cellname = lineSplit[3];
                    nrPciFault.sc_pci = NumberUtils.isNumber(lineSplit[4])?Integer.parseInt(lineSplit[4]) : 0;
                    nrPciFault.sc_fcn = NumberUtils.isNumber(lineSplit[5])?Integer.parseInt(lineSplit[5]) : 0;
                    nrPciFault.sc_city = lineSplit[6];
                    nrPciFault.sc_vendor = lineSplit[7];
                    nrPciFault.problem = lineSplit[8];
                    nrPciFault.nc_enodebid1 = lineSplit[9];
                    nrPciFault.nc_enodebname1 = lineSplit[10];
                    nrPciFault.nc_cellid1 = lineSplit[11];
                    nrPciFault.nc_pci1 = NumberUtils.isNumber(lineSplit[12])?Integer.parseInt(lineSplit[12]) : 0;
                    nrPciFault.nc_fcn1 = NumberUtils.isNumber(lineSplit[13])?Integer.parseInt(lineSplit[13]) : 0;
                    nrPciFault.nc_city1 = lineSplit[14];
                    nrPciFault.nc_vendor1 = lineSplit[15];
                    nrPciFault.nc_enodebid2 = lineSplit[16];
                    nrPciFault.nc_enodebname2 = lineSplit[17];
                    nrPciFault.nc_cellid2 = lineSplit[18];
                    nrPciFault.nc_pci2 = lineSplit[19];
                    nrPciFault.nc_fcn2 = NumberUtils.isNumber(lineSplit[20])?Integer.parseInt(lineSplit[20]) : 0;
                    nrPciFault.nc_city2 = lineSplit[21];
                    nrPciFault.nc_vendor2 = lineSplit[22];
                    nrPciFault.runningDate = time;
                }
                return nrPciFault;
            }
        }).collect();

        return nrPciFaultList;
    }


    private List<NrArgsFault> readAgrsEX(FileSystem fs, final String dateStr, final String time, JavaSparkContext jsc) {
        List<NrArgsFault> nrArgsFaultList = jsc.textFile(Config.conf_parse_agg_argsfault + "/day=" + dateStr).map(new Function<String, NrArgsFault>() {
            @Override
            public NrArgsFault call(String line) throws Exception {
                String[] lineSplit = line.split(",", -1);
                NrArgsFault nrArgsFault = new NrArgsFault();
                if (lineSplit.length > 10) {
                    nrArgsFault.sc_enodebid = lineSplit[0];
                    nrArgsFault.sc_enodebname = lineSplit[1];
                    nrArgsFault.sc_cellid = NumberUtils.isNumber(lineSplit[2])?Integer.parseInt(lineSplit[2]) : 0;
                    nrArgsFault.sc_cellname = lineSplit[3];
                    nrArgsFault.sc_pci = NumberUtils.isNumber(lineSplit[4])?Integer.parseInt(lineSplit[4]) : 0;
                    nrArgsFault.sc_fcn = NumberUtils.isNumber(lineSplit[5])?Integer.parseInt(lineSplit[5]) : 0;
                    nrArgsFault.sc_city = lineSplit[6];
                    nrArgsFault.sc_vendor = lineSplit[7];
                    nrArgsFault.problem = lineSplit[8];
                    nrArgsFault.recommend_pci = Integer.parseInt(lineSplit[9]);
                    nrArgsFault.wrong_configure = lineSplit[10];
                    nrArgsFault.runningDate = time;
                }
                return nrArgsFault;
            }
        }).collect();

    return nrArgsFaultList;
    }



    private void delete(Connection connection, String time, String tableName) throws SQLException {
        try {
            String delSql = "delete from " + tableName + " where runningDate = ?";
            PreparedStatement pstm = connection.prepareStatement(delSql);
            pstm.setString(1, time);
            pstm.executeUpdate();
            pstm.close();
            //提交事务
            connection.commit();

        } catch (Exception e) {
            //事务回滚
            connection.rollback();
            log.error("时间为" + time + "的数据删除失败：" + e.getMessage());
        }

    }


    private void insertConfuseData(Connection connection, String time, List<NrPciFault> nrPciFaultList) throws SQLException {
        try {
            for (NrPciFault nrPciFault : nrPciFaultList) {

                StringBuffer insertSql = new StringBuffer();
                insertSql.append(
                        "INSERT INTO  agg_nr_pci_fault (sc_enodebid,sc_enodebname,sc_cellid,sc_cellname,sc_pci,sc_fcn,sc_city,sc_vendor,problem,nc_enodebid1,nc_enodebname1,nc_cellid1,nc_pci1,nc_fcn1,nc_city1,nc_vendor1,nc_enodebid2,nc_enodebname2,nc_cellid2,nc_pci2,nc_fcn2,nc_city2,nc_vendor2,runningDate) VALUES ");
                insertSql.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
                PreparedStatement pstm = connection.prepareStatement(insertSql.toString());
                pstm.setString(1, nrPciFault.sc_enodebid);
                pstm.setString(2, nrPciFault.sc_enodebname);
                pstm.setInt(3, nrPciFault.sc_cellid);
                pstm.setString(4, nrPciFault.sc_cellname);
                pstm.setInt(5, nrPciFault.sc_pci);
                pstm.setInt(6, nrPciFault.sc_fcn);
                pstm.setString(7, nrPciFault.sc_city);
                pstm.setString(8, nrPciFault.sc_vendor);
                pstm.setString(9, nrPciFault.problem);
                pstm.setString(10, nrPciFault.nc_enodebid1);
                pstm.setString(11, nrPciFault.nc_enodebname1);
                pstm.setString(12, nrPciFault.nc_cellid1);
                pstm.setInt(13, nrPciFault.nc_pci1);
                pstm.setInt(14, nrPciFault.nc_fcn1);
                pstm.setString(15, nrPciFault.nc_city1);
                pstm.setString(16, nrPciFault.nc_vendor1);
                pstm.setString(17, nrPciFault.nc_enodebid2);
                pstm.setString(18, nrPciFault.nc_enodebname2);
                pstm.setString(19, nrPciFault.nc_cellid2);
                pstm.setString(20, nrPciFault.nc_pci2);
                pstm.setInt(21, nrPciFault.nc_fcn2);
                pstm.setString(22, nrPciFault.nc_city2);
                pstm.setString(23, nrPciFault.nc_vendor2);
                pstm.setString(24, nrPciFault.runningDate);
                pstm.executeUpdate();//插入数据
                pstm.close();
            }
            //提交事务
            connection.commit();
        } catch (Exception e) {
            //回滚事务
            connection.rollback();
            log.error(e.getMessage());
        }
    }


    private void insertArgsData(Connection connection, String time, List<NrArgsFault> nrArgsFaultList) throws SQLException {
        try {
            for (NrArgsFault nrArgsFault : nrArgsFaultList) {
                StringBuffer insertSql = new StringBuffer();
                insertSql.append(
                        "INSERT INTO  agg_nr_args_fault (sc_enodebid,sc_enodebname,sc_cellid,sc_cellname,sc_pci,sc_fcn,sc_city,sc_vendor,problem,recommend_pci,wrong_configure,runningDate) VALUES ");
                insertSql.append("(?,?,?,?,?,?,?,?,?,?,?,?);");
                PreparedStatement pstm = connection.prepareStatement(insertSql.toString());
                pstm.setString(1, nrArgsFault.sc_enodebid);
                pstm.setString(2, nrArgsFault.sc_enodebname);
                pstm.setInt(3, nrArgsFault.sc_cellid);
                pstm.setString(4, nrArgsFault.sc_cellname);
                pstm.setInt(5, nrArgsFault.sc_pci);
                pstm.setInt(6, nrArgsFault.sc_fcn);
                pstm.setString(7, nrArgsFault.sc_city);
                pstm.setString(8, nrArgsFault.sc_vendor);
                pstm.setString(9, nrArgsFault.problem);
                pstm.setInt(10, nrArgsFault.recommend_pci);
                pstm.setString(11, nrArgsFault.wrong_configure);
                pstm.setString(12, nrArgsFault.runningDate);

                pstm.executeUpdate();//插入数据
                pstm.close();
            }
            //提交事务
            connection.commit();
        } catch (Exception e) {
            //回滚事务
            connection.rollback();
            log.error(e.getMessage());
        }
    }






}
