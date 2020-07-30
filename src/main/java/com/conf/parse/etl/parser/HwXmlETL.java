package com.conf.parse.etl.parser;

import com.conf.parse.common.PmAnalysisCommon;
import com.conf.parse.common.TaskStartTime;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import com.conf.parse.etl.ETL;
import com.conf.parse.etl.hwobjects.*;
import com.conf.parse.etl.obj.*;
import com.conf.parse.etl.hwhandler.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;


import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

public class HwXmlETL extends ETL {


	@Override
	public String getSrcPath(FileSystem fs, Options options) {
		return Config.conf_parse_src_hw_path;
	}

	@Override
	public void run(FileSystem fs, HiveContext sqlContext, String srcPath, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception {
//所有基站信息
		ArrayList<Function> functionList = new ArrayList<Function>();
		//所有本地小区
		ArrayList<HwLocalCell> hwlocalCellList = new ArrayList<HwLocalCell>();
		//所有BTS3900CELLOP
		ArrayList<BTS3900CELLOP> BTS3900CELLOPList = new ArrayList<BTS3900CELLOP>();
		//所有BTS3900CNOPERATORTA
		ArrayList<BTS3900CNOPERATORTA> BTS3900CNOPERATORTAList = new ArrayList<BTS3900CNOPERATORTA>();
		//所有BTS3900CNOPERATOR
		ArrayList<BTS3900CNOPERATOR> BTS3900CNOPERATORList = new ArrayList<BTS3900CNOPERATOR>();
		//所有邻接小区
		ArrayList<AdjacencyCell> adjacencyCellList = new ArrayList<AdjacencyCell>();
		//所有邻区关系
		ArrayList<NeighborhoodRelationship> relationshiplist = new ArrayList<NeighborhoodRelationship>();

		FileStatus[] directorys = fs.listStatus(new Path(srcPath));
		//		FileStatus[] directorys = fs.listStatus(new Path("C:\\Users\\黄世良\\Desktop\\工作内容\\邻区优化配置解析\\新建文件夹"));

		for (FileStatus directory : directorys) {
			if (directory.isDirectory()) {
				FileStatus[] files = fs.listStatus(directory.getPath());
				for (FileStatus file : files) {
					System.out.println(file.getPath());
					//解压缩 .tar.gz
					TarInputStream tarIn = null;
					FSDataInputStream fsDataInputStream = fs.open(file.getPath());
					parse(functionList, hwlocalCellList,BTS3900CELLOPList, BTS3900CNOPERATORTAList,BTS3900CNOPERATORList,adjacencyCellList, relationshiplist, fsDataInputStream, tarIn);
				}
			}
		}



		System.out.println("所有基站信息："+functionList.size());
		System.out.println("所有本地小区："+hwlocalCellList.size());
		System.out.println("所有BTS3900CELLOP："+BTS3900CELLOPList.size());
		System.out.println("所有BTS3900CNOPERATORTA："+BTS3900CNOPERATORTAList.size());
		System.out.println("所有BTS3900CNOPERATOR："+BTS3900CNOPERATORList.size());
		System.out.println("所有邻接小区："+adjacencyCellList.size());
		System.out.println("所有邻区关系："+relationshiplist.size());
		//把文件写入到hdfs中
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_function + "/HW/HW_FUNCTION.CSV", functionList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_hw_localcell + "/HW_HWLOCALCELL.CSV", hwlocalCellList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_hw_bts3900cellop + "/HW_BTS3900CELLOP.CSV", BTS3900CELLOPList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_hw_bts3900cnoperatorta + "/HW_BTS3900CNOPERATORTA.CSV", BTS3900CNOPERATORTAList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_hw_bts3900cnoperator + "/HW_BTS3900CNOPERATOR.CSV", BTS3900CNOPERATORList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_adjacencycell + "/HW/HW_ADJACENCYCELL.CSV", adjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_neighborrelation + "/HW/HW_NEIGHBORRELATION.CSV", relationshiplist,false);
		/*//把文件写入到hdfs中
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_FUNCTION/HW/HW_20190107_FUNCTION.CSV",functionList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_HW_LOCALCELL/HW_20190107_HWLOCALCELL.CSV",hwlocalCellList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_HW_BTS3900CELLOP/HW_20190107_BTS3900CELLOP.CSV",BTS3900CELLOPList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_HW_BTS3900CNOPERATORTA/HW_20190107_BTS3900CNOPERATORTA.CSV",BTS3900CNOPERATORTAList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_HW_BTS3900CNOPERATOR/HW_20190107_BTS3900CNOPERATOR.CSV",BTS3900CNOPERATORList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_ADJACENCYCELL/HW/HW_20190107_ADJACENCYCELL.CSV",adjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_NEIGHBORRELATION/HW/HW_20190107_NEIGHBORRELATION.CSV",relationshiplist,false);*/

		//得到HW本地小区，得到所有列
		getHwLocalCell(sqlContext);



	}


	private void parse(ArrayList<Function> functionList,
			ArrayList<HwLocalCell> hwlocalCellList,
			ArrayList<BTS3900CELLOP> BTS3900CELLOPList,
			ArrayList<BTS3900CNOPERATORTA> BTS3900CNOPERATORTAList,
			ArrayList<BTS3900CNOPERATOR> BTS3900CNOPERATORList, 
			ArrayList<AdjacencyCell> adjacencyCellList,
			ArrayList<NeighborhoodRelationship> relationshiplist,
			FSDataInputStream fsDataInputStream, 
			TarInputStream tarIn) throws Exception {
		
		
		try {
            tarIn = new TarInputStream(new GZIPInputStream(new BufferedInputStream(fsDataInputStream)), 1024 * 2, "GBK");
            TarEntry tarEn = null;
            while ((tarEn = tarIn.getNextEntry()) != null) {
                if (!tarEn.isDirectory()) {
                    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = tarIn.read(buffer)) > -1) {
                        outStream.write(buffer, 0, len);
                    }
                    outStream.flush();
                  
                    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
                    SAXParser saxParser = saxParserFactory.newSAXParser();
                    saxParser.parse(new ByteArrayInputStream(outStream.toByteArray()), new HwXmlETLHandler(functionList, hwlocalCellList, BTS3900CELLOPList, BTS3900CNOPERATORTAList, BTS3900CNOPERATORList, adjacencyCellList, relationshiplist));
                 
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
            	
                tarIn.close();
                fsDataInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}
	
	/**
	 * 得到总的本地小区，得到所有列
	 */
	public void getHwLocalCell(HiveContext sqlContext)
	{
		sqlContext.sql("use pm");
		sqlContext.sql("INSERT OVERWRITE TABLE ETL_CONF_ANALYZ_LOCALCELL PARTITION (vender = 'HW')"
				+ " SELECT f.*, e.sc_MCC, e.sc_MNC, e.sc_TAC "
				+ "FROM ETL_CONF_ANALYZ_HW_LOCALCELL f "
				+ "LEFT JOIN "
				+ "( SELECT c.sc_ENODEBID, c.sc_LOCALCELLID, c.TRACKINGAREAID, c.CNOPERATORID, c.sc_TAC, d.sc_MCC, d.sc_MNC "
				+ "FROM ETL_CONF_ANALYZ_HW_BTS3900CNOPERATOR d "
				+ "LEFT JOIN "
				+ "( SELECT a.sc_ENODEBID, a.sc_LOCALCELLID, a.TRACKINGAREAID, b.CNOPERATORID, b.sc_TAC "
				+ "FROM ETL_CONF_ANALYZ_HW_BTS3900CELLOP a "
				+ "LEFT JOIN ETL_CONF_ANALYZ_HW_BTS3900CNOPERATORTA b ON a.sc_ENODEBID = b.sc_ENODEBID AND a.TRACKINGAREAID = b.TRACKINGAREAID ) c "
				+ "ON d.sc_ENODEBID = c.sc_ENODEBID AND d.CNOPERATORID = c.CNOPERATORID ) e "
				+ "ON f.sc_ENODEBID = e.sc_ENODEBID AND f.sc_LOCALCELLID = e.sc_LOCALCELLID");
			
	}

	
	

	
	  public static void SumTimes(long startTime,long endTime) throws ParseException {
	         SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	         
	           long l=endTime-startTime;
	           long day=l/(24*60*60*1000);
	           long hour=(l/(60*60*1000)-day*24);
	           long min=((l/(60*1000))-day*24*60-hour*60);
	           long s=(l/1000-day*24*60*60-hour*60*60-min*60);
	           System.out.println(""+day+"天"+hour+"小时"+min+"分"+s+"秒");
	    }

}
