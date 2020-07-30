package com.work.etl.ztehandler;

import com.conf.parse.common.CountDlearfcn;
import com.conf.parse.etl.obj.LocalCell;
import com.conf.parse.etl.zteobjects.*;
import com.work.etl.zteobjects.ZTELocalCell;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

/**
 * 本地小区
 * @author 赖书恒
 *
 */
public class LocalCellHandler extends DefaultHandler{
	public String preTag;//上一个标签名
	//关联关系所需要的类
	public ArrayList<ZTELocalCell> zteLocalCellList;
	public ZTELocalCell zteLocalCell;//中兴的邻区关系对象
	//存表所需要的类
	public ArrayList<LocalCell> localCellList;
	public LocalCell localCell;
	public StringBuilder stringBuilder;
	//主小区频点
	//频点计算1：earfcnUl
	double earfcnUlText = 0;
	//频点计算所用标签：freqBandInd(默认值不严谨)
	int freqBandIndText = 1;
	//频点计算2：earfcnDl
	double earfcnDlText = 0;
	public LocalCellHandler(ArrayList<ZTELocalCell> zteLocalCellList, ArrayList<LocalCell> localCellList) {
		this.zteLocalCellList = zteLocalCellList;
		this.localCellList = localCellList;
	}

	/**
	 * 开始读取xml文档
	 */
	@Override
	public void startDocument() throws SAXException {
	/*	zteLocalCellList=new ArrayList<>();
		localCellList=new ArrayList<>();*/
		
		stringBuilder = new StringBuilder();
	}
	
	//获取本地小区
	public ArrayList<ZTELocalCell> getZteLocalCellList(){
		return zteLocalCellList;
	}
	
	/**
	 * 扫描到开始标签，进入该方法
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		stringBuilder.setLength(0);
		/*
		 *  遇到EUtranRelation节点，new一个对象，
		 *  注意有限制，如果没有不是<EUtranRelation id="188">这种有id的不会new对象，
		 *  这样就防止在遇到EUtranRelation里的EUtranRelation子节点时又new 对象
		 */
		if ("EUtranCellFDD".equals(qName)&&attributes.getQName(0)!=null) {
			// 遇到EUtranRelation节点，new一个对象
			zteLocalCell=new ZTELocalCell();
			localCell = new LocalCell();
		}
		preTag = qName;
	//	System.out.println("-----------------------------" + qName);
	}
	
	
	/**
	 * 扫描到标签中内容，进入该方法
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if (zteLocalCell != null && localCell != null) { //如果不等于null，说明还在ExternalEUtranCellFDD这一整层节点中
			String data = stringBuilder.append(ch, start, length).toString();      
			//本地小区基站号
			if ("ENBFunctionFDD".equals(preTag)) {
				zteLocalCell.sc_ENODEBID = data;
	    		localCell.sc_ENODEBID = data;
			}
	        //本地小区小区编号
	        if ("cellLocalId".equals(preTag)) {
	        	zteLocalCell.sc_LOCALCELLID = data;
		    	localCell.sc_LOCALCELLID = data;
		   	}
		    
			if("refPlmn".equals(preTag)){
				String[] refSplit = data.split(",", -1);
				String[] split;
				if(refSplit.length == 4){
					split = refSplit[0].split("=", -1);
					if(split.length==2)
					{
						zteLocalCell.sc_SUBNETWORK1 = split[1];
					}
					split = refSplit[1].split("=", -1);
					if(split.length==2)
					{
						zteLocalCell.sc_MEID1 = split[1];
					}
					split = refSplit[2].split("=", -1);
					if(split.length==2)
					{
						zteLocalCell.sc_OPERATOR= split[1];
					}
					split = refSplit[3].split("=", -1);
					if(split.length==2)
					{
						zteLocalCell.sc_PLMN = split[1];
					}
		
				}
			}
		    
	    	//本地小区小区编号
	    	if ("userLabel".equals(preTag)) {
	    		zteLocalCell.sc_CELLNAME = data;
	    		localCell.sc_CELLNAME = data;
	    	}
	      	//本地小区制式
	      	zteLocalCell.sc_FDDTDDIND = "FDD";
	      	localCell.sc_FDDTDDIND = "FDD";
	      	//主小区PCI
	      	if ("pci".equals(preTag)) {
	    	  	zteLocalCell.sc_PHYCELLID = data;
	    	  	localCell.sc_PHYCELLID = data;
	      	}
	      
	      	if("tac".equals(preTag)){
	      		zteLocalCell.sc_TAC = data;
	      	}
	      
	      	/*//频点计算1：earfcnUl
	      	if ("earfcnUl".equals(preTag)) {
	      		earfcnUlText = Double.parseDouble(data);
		  	}*/
	      	//频点计算所用标签：freqBandIndText
	      	if ("freqBandInd".equals(preTag)) {
	      		freqBandIndText = Integer.parseInt(data);
		  	}
		  	//频点计算2：earfcnDl
		  	if ("earfcnDl".equals(preTag)) {
			  	earfcnDlText = Double.parseDouble(data);
			}
	      
	      
	      	//主小区关联SubNetwork
	      	if ("SubNetwork".equals(preTag)) {
	    	  	zteLocalCell.sc_SUBNETWORK = data;
	      	}
	      	//主小区关联MEID
	      	if ("MEID".equals(preTag)) {
	      		zteLocalCell.sc_MEID = data;
	      	}
	      	//主小区关联EUtranCellFDD
	      	if ("EUtranCellFDD".equals(preTag)) {
	      		zteLocalCell.sc_EUTRANCELLFDD = data;
	      	}
         
		}
	}

	
	
	/**
	 * 扫描到结束标签，进入该方法
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
		//这里要加判断，主要是因为EUtranRelation这一层父节点中有一个子节点也叫EUtranRelation
		if(qName.equals("EUtranCellFDD")&&!"EUtranCellFDD".equals(preTag))
		{
		//	System.out.println(zteLocalCell);
			try{
			zteLocalCell.sc_DLEARFCN = CountDlearfcn.count(freqBandIndText, earfcnDlText);
			localCell.sc_DLEARFCN = CountDlearfcn.count(freqBandIndText, earfcnDlText);
			}catch(NullPointerException e){
				System.out.println(localCell.sc_CELLNAME + "----" + localCell.sc_CELLID + "---------" + freqBandIndText + "-----" + earfcnDlText);
			}
			zteLocalCellList.add(zteLocalCell);
			localCellList.add(localCell);
			zteLocalCell=null;
			localCell=null;
		}
	
		preTag=null;
	}
	
	/**
	 * 结束
	 */
	@Override
	public void endDocument() throws SAXException {
		try {
			//FileSystem fs = FSUtils.getNewFileSystem();
			/*PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_LOCALCELL/ZTE_LOCALCELL.CSV",localCellList,false);
			PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/SAVE/ETL_CONF_ANALYZ_ZTE_LOCALCELL/ZteLocalCell.csv",zteLocalCellList,false);*/
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
