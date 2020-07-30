package com.conf.parse.etl.parser;

import com.conf.parse.common.*;
import com.conf.parse.config.Config;
import com.conf.parse.config.Options;
import com.conf.parse.etl.ETL;
import com.conf.parse.etl.obj.*;
import com.conf.parse.etl.ztehandler.*;
import com.conf.parse.etl.zteobjects.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;


/**
 * 中兴XML解析
 * @author 赖书恒
 *
 */
public class ZteSaxXmlETL extends ETL {
	// 基站号与地市名称对应字典
	public static Map<String, String> mapEnodebCity;
	public static List<String> list = new ArrayList<>();

	@Override
	public String getSrcPath(FileSystem fs, Options options) {
		return Config.conf_parse_src_zte_path;
	}
	//解析汇总
	@Override
	public void run(FileSystem fs, HiveContext sqlContext, String srcPath, String savePath, TaskStartTime tst, JavaSparkContext jsc) throws Exception {
		//String path = "/user/noce/DATA/PUBLIC/NOCE/SRC/ZTE/1.0.18/20180503/2018050300/1542940456354_bulkcm_zte_20180503010812.zip";
		/*String srcFile = "/user/noce/DATA/PUBLIC/NOCE/SRC/ZTE/1542940456354_bulkcm_zte_20180503010812.zip";*/
		//String srcFile = "/user/noce/DATA/PUBLIC/NOCE/SRC/ZTE";
		//获取基站号和地市的对应关系(到时候要修改)
		//String mapFile = "/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_PM_LTE_BASE_ENODEB/ETL_PM_LTE_BASE_ENODEB.csv";
		String mapFile = Config.conf_parse_etl_base_enodeb + "/ETL_PM_LTE_BASE_ENODEB.csv";
		getAnalysiseFiles(fs, mapFile);
		//所有基站信息
		ArrayList<Function> functionList = new ArrayList<Function>();
		//所有本地小区信息
		ArrayList<LocalCell> localCellList = new ArrayList<LocalCell>();
		//中兴需关联本地小区信息
		ArrayList<ZTELocalCell> zteLocalCellList = new ArrayList<ZTELocalCell>();
		//所有邻接小区信息
		ArrayList<AdjacencyCell> adjacencyCellList = new ArrayList<AdjacencyCell>();
		//中兴需关联邻接小区信息
		ArrayList<ZTEAdjacencyCell> zteAdjacencyCellList = new ArrayList<ZTEAdjacencyCell>();
		//中兴Plmn信息
		ArrayList<ZTEPlmn> ztePlmnList = new ArrayList<ZTEPlmn>();
		//所有邻接关系信息
		ArrayList<ZTENeighborhoodRelationship> zteRelationshiplist = new ArrayList<ZTENeighborhoodRelationship>();
		//邻接表
		ArrayList<NeighborhoodRelationship> neighborhoodRelationshipList = new ArrayList<NeighborhoodRelationship>();

		FileStatus[] directorys = fs.listStatus(new Path(srcPath));
		for (FileStatus directory : directorys) {
			if (directory.isDirectory()) {
				FileStatus[] files = fs.listStatus(directory.getPath());
				for (FileStatus file : files) {
					System.out.println(file.getPath());
					//解压zip包
					InputStream fileStream = FSUtils.openInputStream(fs, file.getPath().toString());
					//FSDataInputStream fileStream = fs.open(file.getPath());
					ZipInputStream zipin = null;
					try {
						zipin = new ZipInputStream(new BufferedInputStream(fileStream));
						ZipEntry zipEn = null;
						ZipFile zipFile = null;
						//Document document;
						while((zipEn = zipin.getNextEntry()) != null){
							if(!zipEn.isDirectory()){
								//拿到xml的文件名
								String[] fileSplit = zipEn.getName().split("/");
								//1、创建sax解析器工厂
								SAXParserFactory factory=SAXParserFactory.newInstance();

								//2、创建sax解析器
								SAXParser parser=factory.newSAXParser();

								// 1、创建解析器（dom方式）
								SAXReader reader = new SAXReader();
								Document document;

								//解析基站表的XML文件(用dom方式解析)
								if(fileSplit[1].equals("ENBFunctionFDD.xml")){
									document = reader.read(new XmlZipInputStream(zipin));
									parseFunctionXML(document, functionList);
									//FunctionFDDHandler functionFDDHandler = new FunctionFDDHandler(functionList);
									//parser.parse(new XmlZipInputStream(zipin), functionFDDHandler);
								}
								//测试本地小区
								if(fileSplit[1].equals("EUtranCellFDD.xml")){
									LocalCellHandler localCellHandler = new LocalCellHandler(zteLocalCellList, localCellList);
									parser.parse(new XmlZipInputStream(zipin), localCellHandler);
								}
								//测试邻接小区
								if(fileSplit[1].equals("ExternalEUtranCellFDD.xml")){
									AdjacencyCellHandler adjacencyCellHandler = new AdjacencyCellHandler(zteAdjacencyCellList, adjacencyCellList);
									parser.parse(new XmlZipInputStream(zipin), adjacencyCellHandler);
								}
								//测试Plmn.xml
								if(fileSplit[1].equals("Plmn.xml")){
									PlmnHandler plmnHandler = new PlmnHandler(ztePlmnList);
									parser.parse(new XmlZipInputStream(zipin), plmnHandler);
								}
								//测试邻接关系
								if(fileSplit[1].equals("EUtranRelation.xml")){
									NeighborhoodRelationshipHandler neighborhoodRelationshipHandler =  new NeighborhoodRelationshipHandler(zteRelationshiplist);
									parser.parse(new XmlZipInputStream(zipin), neighborhoodRelationshipHandler);
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}finally {

						if (zipin != null) {
							zipin.close();
						}
						if (fileStream != null) {
							fileStream.close();
						}

					}

				}
			}
		}
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_function + "/ZTE/ZTE_FUNCTION.CSV", functionList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_zte_localcell + "/ZteLocalCell.CSV", zteLocalCellList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_adjacencycell + "/ZTE/ZTE_ADJACENCYCELL.CSV", adjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_zte_adjacencycell + "/ZteAdjacencyCell.csv", zteAdjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_zte_neighborrelation + "/ZteRelationship.csv", zteRelationshiplist,false);
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_zte_plmn + "/Plmn.csv", ztePlmnList,false);
		/*PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_FUNCTION/ZTE/ZTE_20190107_FUNCTION.CSV",functionList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_ZTE_LOCALCELL/ZteLocalCell.csv",zteLocalCellList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_ZTE_PLMN/Plmn.csv",ztePlmnList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_ADJACENCYCELL/ZTE/ZTE_20190107_ADJACENCYCELL.CSV",adjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_ZTE_ADJACENCYCELL/ZteAdjacencyCell.csv",zteAdjacencyCellList,false);
		PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_ZTE_NEIGHBORRELATION/ZteRelationship.csv",zteRelationshiplist,false);*/

		//根据中间表获取本地小区表
		getZteLocalCell(fs, sqlContext);

		//根据中间表获取中兴关联关系表
		getRelation(fs, sqlContext);

	}

	
	
	
	private void getAnalysiseFiles(FileSystem fs, String mapFile) throws Exception {
		mapEnodebCity = new HashMap<String, String>();
		FSDataInputStream mapInputStream = fs.open(new Path(mapFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(mapInputStream));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] split = line.split(",");
			String cityName = split[3];
			String enodebid = split[4];
			if(!mapEnodebCity.keySet().contains(enodebid)){
				mapEnodebCity.put(enodebid, cityName);
			}
		}
	}




	/**
	 * 解析xml，得到基站信息
	 * @param document
	 * @param functionList
	 * @throws Exception 
	 */
	private void parseFunctionXML(Document document, ArrayList<Function> functionList) throws Exception {
		//主小区基站节点
		List<Node> nodes = document.selectNodes("//ENBFunctionFDD_List//ENBFunctionFDD[@id]");
		for (Node node : nodes) {
			//基站信息
			Function function = new Function();
		
			//主小区基站号
			Node enodebid = node.selectSingleNode(".//eNBId");
			if(enodebid != null){				
				function.sc_ENODEBID = enodebid.getText();
				
			}		
			//主小区基站名 
			Node enodebfunctionname = node.selectSingleNode(".//enbName");
			if(enodebfunctionname != null){
				function.sc_ENODEBFUNCTIONNAME = enodebfunctionname.getText();		
			}
			//主小区地市
			function.sc_LOCATIONNAME = mapEnodebCity.get(function.sc_ENODEBID);
			//主小区厂家
			function.sc_VENDORNAME = "ZTE";
			if(function.sc_ENODEBID != null && function.sc_ENODEBID.length() == 6){	
				functionList.add(function);	
			}
		}
	}


	//根据中间表获取本地小区表
	public void getZteLocalCell(FileSystem fs, HiveContext sqlContext) throws Exception{
		ArrayList<LocalCell> localCellList = new ArrayList<LocalCell>();
		String sqlText1 = "use pm";
		sqlContext.sql(sqlText1);
		String sqlText = "SELECT A.sc_ENODEBID, A.sc_LOCALCELLID, A.sc_CELLNAME, A.sc_FDDTDDIND, A.sc_PHYCELLID, A.sc_DLEARFCN, B.sc_MCC, B.sc_MNC, A.sc_TAC FROM "
				+ "(SELECT * FROM ETL_CONF_ANALYZ_ZTE_LOCALCELL) A "
				+ "JOIN ETL_CONF_ANALYZ_ZTE_PLMN B ON "
				+ "A.sc_SUBNETWORK1 = B.sc_SUBNETWORK1 and "
				+ "A.sc_MEID1 = B.sc_MEID1 and "
				+ "A.sc_OPERATOR = B.sc_OPERATOR and "
				+ "A.sc_PLMN = B.sc_PLMN";
		DataFrame relation = sqlContext.sql(sqlText);
		for (Row row : relation.collect()) {
			if(row.anyNull())
				continue;
			LocalCell localCell = new LocalCell();
			//主小区基站号
			localCell.sc_ENODEBID = row.getString(0);
			//主小区小区编号
			localCell.sc_LOCALCELLID = row.getString(1);
			//主小区小区名称
			localCell.sc_CELLNAME = row.getString(2);
			//主小区制式
			localCell.sc_FDDTDDIND = row.getString(3);
			//主小区PCI
			localCell.sc_PHYCELLID = row.getString(4);
			//主小区频点
			localCell.sc_DLEARFCN = row.getString(5);
			//主小区MCC
			localCell.sc_MCC = row.getString(6);
			//主小区MNC
			localCell.sc_MNC = row.getString(7);
			//主小区TAC
			localCell.sc_TAC = row.getString(8);
			localCellList.add(localCell);
		}
		PmAnalysisCommon.pmFileWrite(fs,Config.conf_parse_etl_localcell + "/ZTE/ZTE_LOCALCELL.CSV",localCellList,false);
		//PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_LOCALCELL/ZTE/ZTE_20190107_LOCALCELL.CSV",localCellList,false);
	}


	//根据中间表获取中兴关联关系表
	public void getRelation(FileSystem fs, HiveContext sqlContext) throws Exception{
		ArrayList<NeighborhoodRelationship> neighborhoodRelationList = new ArrayList<NeighborhoodRelationship>();
		String sqlText1 = "use pm";
		sqlContext.sql(sqlText1);
		String sqlText = "SELECT  C.sc_ENODEBID, C.sc_LOCALCELLID, A.nc_ENODEBID, A.nc_CELLID FROM ETL_CONF_ANALYZ_ZTE_NEIGHBORRELATION B "
				+ "JOIN ETL_CONF_ANALYZ_ZTE_ADJACENCYCELL A "
				+ "JOIN ETL_CONF_ANALYZ_ZTE_LOCALCELL C "
				+ "ON(B.nc_SUBNETWORK = A.nc_SUBNETWORK and "
				+ "B.nc_MEID = A.nc_MEID and "
				+ "B.nc_ENBFUNCTIONFDD = A.nc_ENBFUNCTIONFDD and "
				+ "B.nc_EXTERNALEUTRANCELLFDD = A.nc_EXTERNALEUTRANCELLFDD and "
				+ "B.sc_SUBNETWORK = C.sc_SUBNETWORK and "
				+ "B.sc_MEID = C.sc_MEID and "
				+ "B.sc_ENBFUNCTIONFDD = C.sc_ENODEBID and "
				+ "B.sc_EUTRANCELLFDD = C.sc_EUTRANCELLFDD)";
		DataFrame relation = sqlContext.sql(sqlText);
		for (Row row : relation.collect()) {
			if(row.anyNull())
				continue;
			NeighborhoodRelationship neighborhoodRelationship = new NeighborhoodRelationship();
			//邻接关系主小区基站号
			neighborhoodRelationship.sc_ENODEBID = row.getString(0);
			//邻接关系主小区小区编号
			neighborhoodRelationship.sc_LOCALCELLID = row.getString(1);
			//邻接关系邻接小区基站号
			neighborhoodRelationship.nc_ENODEBID = row.getString(2);
			//邻接关系邻接小区小区编号
			neighborhoodRelationship.nc_CELLID = row.getString(3);
			neighborhoodRelationList.add(neighborhoodRelationship);
		}
		PmAnalysisCommon.pmFileWrite(fs, Config.conf_parse_etl_neighborrelation + "/ZTE/ZTE_NEIGHBORRELATION.CSV",neighborhoodRelationList,false);
		//PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_NEIGHBORRELATION/ZTE/ZTE_20190107_NEIGHBORRELATION.CSV",neighborhoodRelationList,false);
	}


}





