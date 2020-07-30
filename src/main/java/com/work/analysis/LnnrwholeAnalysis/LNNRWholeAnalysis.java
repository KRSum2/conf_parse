package com.work.analysis.LnnrwholeAnalysis;

import com.conf.parse.analysis.Analysis;
import com.conf.parse.analysis.obj.LNNRWhole;
import com.conf.parse.common.FSUtils;
import com.conf.parse.common.PmAnalysisCommon;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.conf.parse.etl.obj.*;
import com.work.analysis.Analysis;
import com.work.analysis.obj.LNNRWhole;
import com.work.common.FSUtils;
import com.work.common.TaskStartTime;
import com.work.config.Config;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.BufferedInputStream;
import java.util.ArrayList;

public class LNNRWholeAnalysis extends Analysis {

    @Override
    public void run(FileSystem fs, HiveContext sqlContext, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception {
        //获取总基站表
        getFunction(fs, sqlContext);
        //获取总本地小区表
        getLocalCell(fs, sqlContext);
        //获取总外接小区表
        getAdjacencyCell(fs, sqlContext);
        //获取总关联关系表
        getNeighborhoodRelationship(fs, sqlContext);
        //获取总表
        getLNNRWhole(fs, sqlContext);
        getFinalData(fs);
    }

    //获取基站总表
    public void getFunction(FileSystem fs, HiveContext sqlContext) throws Exception{
        ArrayList<Function> functionList = new ArrayList<Function>();
        String sqlText1 = "use pm";
        sqlContext.sql(sqlText1);
        String sqlText = "SELECT enodebid, enodebfunctionname, locationname, vendorname FROM ETL_CONF_ANALYZ_FUNCTION ";
        DataFrame relation = sqlContext.sql(sqlText);
        for (Row row : relation.collect()) {
            if(row.anyNull())
                continue;
            Function function = new Function();
            //基站号
            function.sc_ENODEBID = row.getString(0);
            //基站名称
            function.sc_ENODEBFUNCTIONNAME = row.getString(1);
            //地市
            function.sc_LOCATIONNAME = row.getString(2);
            //厂家
            function.sc_VENDORNAME = row.getString(3);
            functionList.add(function);
        }
        PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_DATA/Function.CSV",functionList,false);
    }

    //获取邻接小区总表
    public void getAdjacencyCell(FileSystem fs, HiveContext sqlContext) throws Exception{
        ArrayList<AdjacencyCell> adjacencyCellList = new ArrayList<AdjacencyCell>();
        String sqlText1 = "use pm";
        sqlContext.sql(sqlText1);
        String sqlText = "SELECT nc_enodebid, nc_cellid, nc_phycellid, nc_dlearfcn, nc_mcc, nc_mnc, nc_tac FROM ETL_CONF_ANALYZ_ADJACENCYCELL ";
        DataFrame relation = sqlContext.sql(sqlText);
        for (Row row : relation.collect()) {
            if(row.anyNull())
                continue;
            AdjacencyCell adjacencyCell = new AdjacencyCell();
            //邻接小区基站号
            adjacencyCell.nc_ENODEBID = row.getString(0);
            //邻接小区小区编号
            adjacencyCell.nc_CELLID = row.getString(1);
            //邻接小区pci
            adjacencyCell.nc_PHYCELLID = row.getInt(2) + "";
            //邻接小区频点
            adjacencyCell.nc_DLEARFCN = row.getInt(3) + "";
            adjacencyCell.nc_MCC = row.getString(4);
            adjacencyCell.nc_MNC = row.getString(5);
            adjacencyCell.nc_TAC = row.getString(6);
            adjacencyCellList.add(adjacencyCell);
        }
        PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_DATA/AdjacencyCell.CSV",adjacencyCellList,false);
    }

    //获取本地小区总表
    public void getLocalCell(FileSystem fs, HiveContext sqlContext) throws Exception{
        ArrayList<LocalCell> localCellList = new ArrayList<LocalCell>();
        String sqlText1 = "use pm";
        sqlContext.sql(sqlText1);
        String sqlText = "SELECT sc_enodebid, sc_cellid, sc_localcellid, sc_cellname, sc_fddtddind, sc_phycellid, sc_dlearfcn, sc_mcc, sc_mnc, sc_tac FROM ETL_CONF_ANALYZ_LOCALCELL ";
        DataFrame relation = sqlContext.sql(sqlText);
        for (Row row : relation.collect()) {
            if(row.anyNull())
                continue;
            LocalCell localCell = new LocalCell();
            //主小区基站号
            localCell.sc_ENODEBID = row.getString(0);
            localCell.sc_CELLID = row.getInt(1) + "";
            //主小区小区编号
            localCell.sc_LOCALCELLID = row.getInt(2) + "";
            //主小区小区名称
            localCell.sc_CELLNAME = row.getString(3);
            //主小区制式
            localCell.sc_FDDTDDIND = row.getString(4);
            //主小区PCI
            localCell.sc_PHYCELLID = row.getInt(5) + "";
            //主小区频点
            localCell.sc_DLEARFCN = row.getInt(6) + "";
            localCell.sc_MCC = row.getString(7);
            localCell.sc_MNC = row.getString(8);
            localCell.sc_TAC = row.getString(9);
            localCellList.add(localCell);
        }
        PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_DATA/LocalCell.CSV",localCellList,false);
    }

    //获取关联关系总表
    public void getNeighborhoodRelationship(FileSystem fs, HiveContext sqlContext) throws Exception{

        ArrayList<NeighborhoodRelationship> neighborhoodRelationshipList = new ArrayList<NeighborhoodRelationship>();
        String sqlText1 = "use pm";
        sqlContext.sql(sqlText1);
        String sqlText = "SELECT sc_enodebid, sc_localcellid, nc_enodebid, nc_cellid FROM ETL_CONF_ANALYZ_NEIGHBORRELATION ";
        DataFrame relation = sqlContext.sql(sqlText);
        for (Row row : relation.collect()) {
            if(row.anyNull())
                continue;
            NeighborhoodRelationship neighborhoodRelationship = new NeighborhoodRelationship();
            //主小区基站号
            neighborhoodRelationship.sc_ENODEBID = row.getString(0);
            //主小区小区编号
            neighborhoodRelationship.sc_LOCALCELLID = row.getString(1);
            //邻接小区基站号
            neighborhoodRelationship.nc_ENODEBID = row.getString(2);
            //邻接小区编号
            neighborhoodRelationship.nc_CELLID = row.getString(3);
            neighborhoodRelationshipList.add(neighborhoodRelationship);
        }
        PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_DATA/NeighborhoodRelationship.CSV",neighborhoodRelationshipList,false);
    }

    //获取总表
    public void getLNNRWhole(FileSystem fs, HiveContext sqlContext) throws Exception{

        ArrayList<LNNRWhole> lnnrWholeList = new ArrayList<LNNRWhole>();
        String sqlText1 = "use pm";
        sqlContext.sql(sqlText1);
        String sqlText = "WITH analyz_function AS ( SELECT * FROM etl_conf_analyz_function WHERE length(ENODEBID) == 6 ) INSERT overwrite TABLE AGG_CONF_ANALYZ_LNNRWHOLE SELECT * FROM ( SELECT DISTINCT function_local.sc_ENODEBID, function_local.sc_ENODEBFUNCTIONNAME, CASE WHEN function_local.sc_CELLID IS NOT NULL THEN function_local.sc_CELLID ELSE function_local.sc_LOCALCELLID END AS sc_LOCALCELLID, SC_CELLNAME, sc_fddtddind, sc_PHYCELLID, sc_DLEARFCN, sc_LOCATIONNAME, sc_VENDORNAME, sc_MCC, sc_MNC, sc_TAC, function_adjacency.nc_enodebid, nc_ENODEBFUNCTIONNAME, function_adjacency.nc_cellid, nc_PHYCELLID, nc_DLEARFCN, nc_LOCATIONNAME, nc_vendorname, nc_MCC, nc_MNC, nc_TAC, CASE WHEN sc_DLEARFCN = nc_DLEARFCN THEN 'INTRA' ELSE 'INTER' END AS sc_nc_NR FROM ( SELECT * FROM etl_conf_analyz_neighborrelation WHERE length(sc_ENODEBID) == 6 AND length(nc_ENODEBID) == 6 ) neighborrelation JOIN ( SELECT a.sc_ENODEBID, b.ENODEBFUNCTIONNAME sc_ENODEBFUNCTIONNAME, a.sc_CELLID, a.sc_LOCALCELLID, a.SC_CELLNAME, a.sc_fddtddind, a.sc_PHYCELLID, a.sc_DLEARFCN, b.LOCATIONNAME sc_LOCATIONNAME, b.VENDORNAME sc_vendorname, a.sc_MCC, a.sc_MNC, a.sc_TAC FROM ( SELECT * FROM etl_conf_analyz_localcell WHERE length(sc_ENODEBID) == 6 ) a LEFT JOIN analyz_function b ON a.sc_ENODEBID = b.ENODEBID ) AS function_local ON ( neighborrelation.sc_enodebid = function_local.sc_ENODEBID AND neighborrelation.sc_localcellid = function_local.sc_LOCALCELLID ) JOIN ( SELECT bb.nc_ENODEBID, aa.ENODEBFUNCTIONNAME nc_ENODEBFUNCTIONNAME, bb.nc_cellid, bb.nc_PHYCELLID, bb.nc_DLEARFCN, aa.LOCATIONNAME nc_LOCATIONNAME, aa.VENDORNAME nc_vendorname, bb.nc_MCC, bb.nc_MNC, bb.nc_TAC FROM analyz_function aa RIGHT JOIN ( SELECT * FROM etl_conf_analyz_adjacencycell WHERE length(nc_ENODEBID) == 6 ) bb ON aa.ENODEBID = bb.nc_ENODEBID ) AS function_adjacency ON ( neighborrelation.nc_enodebid = function_adjacency.nc_enodebid AND neighborrelation.nc_cellid = function_adjacency.nc_cellid )) eee WHERE eee.nc_PHYCELLID IS NOT NULL AND eee.sc_ENODEBFUNCTIONNAME IS NOT NULL AND eee.sc_LOCATIONNAME IS NOT NULL AND eee.sc_VENDORNAME IS NOT NULL AND eee.nc_ENODEBFUNCTIONNAME IS NOT NULL AND eee.nc_LOCATIONNAME IS NOT NULL AND eee.nc_VENDORNAME IS NOT NULL";
        DataFrame relation= sqlContext.sql(sqlText);
        relation.count();
    }


    public void getFinalData(FileSystem fs) throws Exception{
        Path path=new Path(Config.conf_parse_agg_lnnrwhole);
        FileStatus[] listStatus = fs.listStatus(path);
        FSDataOutputStream outputStream = FSUtils.openOutputStream(fs, path+"/LNNRWHOLE.csv");
        byte[] buffer = new byte[1024];
        for (FileStatus file : listStatus) {
            if(file.getPath().getName().startsWith("part")){
                BufferedInputStream bi = new BufferedInputStream(FSUtils.openInputStream(fs, file.getPath().toString()));
                int len;
                while ((len = bi.read(buffer)) > -1) {
                    outputStream.write(buffer, 0, len);
                }
                outputStream.flush();
//	            fs.delete(file.getPath());//删除文件
                fs.delete(file.getPath(), false);//删除文件,不递归

            }
        }

    }


}
