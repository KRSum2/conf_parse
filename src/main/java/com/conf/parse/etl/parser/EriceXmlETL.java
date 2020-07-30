package com.conf.parse.etl.parser;

import com.conf.parse.common.PmAnalysisCommon;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import com.conf.parse.etl.ETL;
import com.conf.parse.etl.ericehandler.EriceHadler;
import com.conf.parse.etl.obj.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * 爱立信配置解析xml
 *
 * @Autohr：PJS
 */
public class EriceXmlETL extends ETL {

    //保存基站信息，服务小区表，邻区小区表，邻区关系表的数据list集合
    List<Function> functionList = new ArrayList<>();            //存放基站list集合
    List<LocalCell> localCellList = new ArrayList<>();          //服务小区list集合
    List<AdjacencyCell> adjacencyCellList = new ArrayList<>();  //邻接小区list结婚
    List<NeighborhoodRelationship> neighborhoodRelationshipList = new ArrayList<>();    //邻区关系list结

    @Override
    public String getSrcPath(FileSystem fs, Options options) {
        return Config.conf_parse_src_ericsson_path;
    }

    @Override
    public void run(FileSystem fs, HiveContext sqlContext, String srcPath, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception {
        //解压压缩包
        FileStatus[] fileStatuses = fs.listStatus(new Path(srcPath));
        //FileStatus[] fileStatuses = fs.listStatus(new Path("E:\\实习全部材料\\XML解析\\邻区优化配置解析\\Ericsson_CM_201805030000_1\\ftp文件"));

        //FSDataInputStream fsDataInputStream = fs.open(new Path("/user/noce/DATA/PUBLIC/NOCE/SRC/ERICSSON/2018052106_ctoss_cm_export.xml.gz"));
        //FSDataInputStream fsDataInputStream = fs.open(new Path("E:\\实习全部材料\\XML解析\\邻区优化配置解析\\2018052106_ctoss_cm_export.xml.gz"));


        //初始化SAX工厂包装
        SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        SAXParser saxParser = saxParserFactory.newSAXParser();

        for (FileStatus fileStatus : fileStatuses){
            if (fileStatus.isDirectory()){
                FileStatus[] fileStatuses1 = fs.listStatus(fileStatus.getPath());
                for (FileStatus fileStatus1 : fileStatuses1){
                    System.out.println(fileStatus1.getPath());
                    FSDataInputStream fsDataInputStream = fs.open(fileStatus1.getPath());
                    GZIPInputStream gzipInputStream = new GZIPInputStream(fsDataInputStream);
                    saxParser.parse(gzipInputStream, new EriceHadler(functionList, localCellList, adjacencyCellList, neighborhoodRelationshipList));
                    gzipInputStream.close();
                    fsDataInputStream.close();
                }
            }
        }

        //邻接小区list进行去重复

        List<String> stringList = new ArrayList<>();


        for (AdjacencyCell adjacencyCell : adjacencyCellList) {
            stringList.add(adjacencyCell.nc_ENODEBID + "," + adjacencyCell.nc_CELLID + "," + adjacencyCell.nc_DLEARFCN);
        }

        HashSet h = new HashSet(stringList);
        stringList.clear();
        stringList.addAll(h);

        adjacencyCellList.clear();

        for (int i = 0; i < stringList.size(); ++i) {
            String string = stringList.get(i);
            String[] strings = string.split(",");
            AdjacencyCell adjacencyCell = new AdjacencyCell();
            adjacencyCell.nc_ENODEBID = strings[0];
            adjacencyCell.nc_CELLID = strings[1];
            adjacencyCell.nc_DLEARFCN = strings[2];
            adjacencyCell.nc_PHYCELLID = "";
            adjacencyCellList.add(adjacencyCell);
        }

        //写文件操作
        PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_function + "/ERICE/ERICE_FUNCTION.CSV", functionList, false); //基站信息
        PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_localcell + "/ERICE/ERICE_LOCALCELL.CSV", localCellList, false); //服务小区
        PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_adjacencycell + "/ERICE/ERICE_ADJACENCYCELL.CSV", adjacencyCellList, false); //邻接小区
        PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_neighborrelation + "/ERICE/ERICE_NEIGHBORRELATION.CSV", neighborhoodRelationshipList, false); //邻区关系



        /*//写文件操作
        PmAnalysisCommon.pmFileWrite(fs, "/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_FUNCTION/ERICE/ERICE_FUNCTION.CSV", functionList, false); //基站信息
        PmAnalysisCommon.pmFileWrite(fs, "/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_LOCALCELL/ERICE/ERICE_LOCALCELL.CSV", localCellList, false); //服务小区
        PmAnalysisCommon.pmFileWrite(fs, "/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_ADJACENCYCELL/ERICE/ERICE_ADJACENCYCELL.CSV", adjacencyCellList, false); //邻接小区
        PmAnalysisCommon.pmFileWrite(fs, "/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_NEIGHBORRELATION/ERICE/ERICE_NEIGHBORRELATION.CSV", neighborhoodRelationshipList, false); //邻区关系*/

        getEricAdjacencyCell(fs, sqlContext);

    }


    // 获取爱立信邻接小区pci
    public void getEricAdjacencyCell(FileSystem fs, HiveContext sqlContext) throws Exception {
        String sqlText = "SELECT b.NC_ENODEBID, b.NC_CELLID, SC_PHYCELLID, nc_dlearfcn, sc_mcc, sc_mnc, sc_tac FROM etl_conf_analyz_localcell a JOIN ( SELECT * FROM etl_conf_analyz_adjacencycell WHERE vender = 'ERICE' ) b ON a.sc_enodebid = b.nc_enodebid AND a.sc_localcellid = b.nc_cellid";
        sqlContext.sql("use pm");
        DataFrame dataFrame = sqlContext.sql(sqlText);
        Row[] collect = dataFrame.collect();
        List<AdjacencyCell> list = new ArrayList<>();

        for (Row row : collect) {
            AdjacencyCell adjacencyCell = new AdjacencyCell();
            for (int i = 0; i < row.length(); ++i) {
                if (row.isNullAt(i)) {
                    continue;
                }
                switch (i) {
                    case 0:
                        adjacencyCell.nc_ENODEBID = (String) row.get(0);
                        break;
                    case 1:
                        adjacencyCell.nc_CELLID = (String) row.get(1);
                        break;
                    case 2:
                        adjacencyCell.nc_PHYCELLID = new Integer((int) (row.get(2))).toString();
                        break;
                    case 3:
                        adjacencyCell.nc_DLEARFCN = new Integer((int) (row.get(3))).toString();
                        break;
                    case 4:
                        adjacencyCell.nc_MCC =  row.getString(4);
                        break;
                    case 5:
                        adjacencyCell.nc_MNC =  row.getString(5);
                        break;
                    case 6:
                        adjacencyCell.nc_TAC =  row.getString(6);
                        break;
                    default:
                        break;
                }
            }
            list.add(adjacencyCell);
        }
        PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_adjacencycell + "/ERICE/ERICE_ADJACENCYCELL.CSV", list, false);

    }



}
