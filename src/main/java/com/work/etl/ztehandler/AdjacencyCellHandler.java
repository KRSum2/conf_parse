package com.work.etl.ztehandler;

import com.conf.parse.common.CountDlearfcn;
import com.conf.parse.etl.obj.AdjacencyCell;
import com.conf.parse.etl.zteobjects.*;
import com.work.etl.zteobjects.ZTEAdjacencyCell;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

/**
 * 邻接小区
 * @author 赖书恒
 *
 */
public class AdjacencyCellHandler extends DefaultHandler {
	public String preTag;//上一个标签名
	//邻接关系所需要的类
	public ArrayList<ZTEAdjacencyCell> zteAdjacencyCellList;
	public ZTEAdjacencyCell zteAdjacencyCell;//中兴的邻接小区对象
	//存储邻接小区信息
	public ArrayList<AdjacencyCell> adjacencyCellList;
	public AdjacencyCell adjacencyCell;
	public StringBuilder stringBuilder;
	//邻接小区频点
	//频点计算所用标签：freqBandInd
	int freqBandIndText = 1;
	//频点计算2：earfcnDl
	public double earfcnDlText = 0;
	
	public AdjacencyCellHandler(ArrayList<ZTEAdjacencyCell> zteAdjacencyCellList,
			ArrayList<AdjacencyCell> adjacencyCellList) {
		this.zteAdjacencyCellList = zteAdjacencyCellList;
		this.adjacencyCellList = adjacencyCellList;
	}

	/**
	 * 开始读取xml文档
	 */
	@Override
	public void startDocument() throws SAXException {
		stringBuilder = new StringBuilder();
	}
	
	//获取邻接小区
	public ArrayList<ZTEAdjacencyCell> getZteAdjacencyCellList(){
		return zteAdjacencyCellList;
	}
	
	/**
	 * 扫描到开始标签，进入该方法
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		
		stringBuilder.setLength(0);
		/*
		 *  遇到ExternalEUtranCellFDD节点，new一个对象，
		 *  注意有限制，如果没有不是<ExternalEUtranCellFDD id="188">这种有id的不会new对象，
		 *  这样就防止在遇到ExternalEUtranCellFDD里的ExternalEUtranCellFDD子节点时又new 对象
		 */
		if ("ExternalEUtranCellFDD".equals(qName)&&attributes.getQName(0)!=null) {			
			zteAdjacencyCell = new ZTEAdjacencyCell();
			adjacencyCell = new AdjacencyCell();
		}
		preTag = qName;
	}
	
	
	/**
	 * 扫描到标签中内容，进入该方法
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		
		if (zteAdjacencyCell != null && adjacencyCell != null) {  //如果不等于null，说明还在ExternalEUtranCellFDD这一整层节点中
			String data = stringBuilder.append(ch, start, length).toString();
            if ("SubNetwork".equals(preTag)) {
                zteAdjacencyCell.nc_SUBNETWORK = data;
            }
            if ("MEID".equals(preTag)) {
                zteAdjacencyCell.nc_MEID = data;
            }
            if ("ENBFunctionFDD".equals(preTag)) {
                zteAdjacencyCell.nc_ENBFUNCTIONFDD = data;
                
            }
            //这里要加判断，主要是因为ExternalEUtranCellFDD这一层父节点中有一个子节点也叫ExternalEUtranCellFDD
            if ("ExternalEUtranCellFDD".equals(preTag)&&"ExternalEUtranCellFDD".equals(preTag)) {
                zteAdjacencyCell.nc_EXTERNALEUTRANCELLFDD = data;
            }
            if("mcc".equals(preTag)){
            	adjacencyCell.nc_MCC = data;
            }
            if("mnc".equals(preTag)){
            	adjacencyCell.nc_MNC = data;
            }
            if ("eNBId".equals(preTag)) {
                zteAdjacencyCell.nc_ENODEBID = data;
                adjacencyCell.nc_ENODEBID = data;
            }
            if ("cellLocalId".equals(preTag)) {
                zteAdjacencyCell.nc_CELLID = data;
                adjacencyCell.nc_CELLID = data;            
            }
            if ("pci".equals(preTag)) {
               zteAdjacencyCell.nc_PHYCELLID = data;
               adjacencyCell.nc_PHYCELLID = data;
            }
            if("tac".equals(preTag)){
            	adjacencyCell.nc_TAC = data;
            }
	      	if ("freqBandInd".equals(preTag)) {
	      		freqBandIndText = Integer.parseInt(data);
		  	}
            if ("earfcnDl".equals(preTag)) {
                earfcnDlText = Double.parseDouble(data);
            }
           
        }
	}

	
	/**
	 * 扫描到结束标签，进入该方法
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
		//这里要加判断，主要是因为ExternalEUtranCellFDD这一层父节点中有一个子节点也叫ExternalEUtranCellFDD
		if(qName.equals("ExternalEUtranCellFDD")&&!"ExternalEUtranCellFDD".equals(preTag))
		{
			try{
				zteAdjacencyCell.nc_DLEARFCN = CountDlearfcn.count(freqBandIndText, earfcnDlText);
				adjacencyCell.nc_DLEARFCN = CountDlearfcn.count(freqBandIndText, earfcnDlText);
				}catch(NullPointerException e){
					System.out.println("freqBandInd不匹配(1,3,5,26)的邻接小区：" + adjacencyCell.nc_CELLID + "----" + adjacencyCell.nc_ENODEBID + "---------" + freqBandIndText + "-----" + earfcnDlText);
				}
			//System.out.println(zteAdjacencyCell);
			zteAdjacencyCellList.add(zteAdjacencyCell);
			adjacencyCellList.add(adjacencyCell);
			zteAdjacencyCell=null; 
			adjacencyCell = null;
		}
		
		preTag=null;
	}
	
	
	/**
	 * 结束
	 */
	@Override
	public void endDocument() throws SAXException {
		try {
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
