package com.conf.parse.etl.ztehandler;

import com.conf.parse.etl.obj.NeighborhoodRelationship;
import com.conf.parse.etl.zteobjects.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

/**
 * 邻接关系
 * @author 赖书恒
 *
 */
public class NeighborhoodRelationshipHandler extends DefaultHandler
{
	public String preTag;//上一个标签名
	public ArrayList<ZTENeighborhoodRelationship> zteRelationshiplist;
	public ZTENeighborhoodRelationship zteNeighborhoodRelationship;//中兴的邻区关系对象
	public StringBuilder stringBuilder;
	//写出的邻接关系
	public ArrayList<NeighborhoodRelationship> neighborhoodRelationshipList;

	
	public NeighborhoodRelationshipHandler(ArrayList<ZTENeighborhoodRelationship> zteRelationshiplist) {
		this.zteRelationshiplist = zteRelationshiplist;
	}

	/**
	 * 开始读取xml文档
	 */
	@Override
	public void startDocument() throws SAXException {
		stringBuilder = new StringBuilder();
	}
	
	//获取关联关系
	public ArrayList<ZTENeighborhoodRelationship> getZteRelationshiplist(){
		return zteRelationshiplist;
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
		if ("EUtranRelation".equals(qName)&&attributes.getQName(0)!=null) {
			// 遇到EUtranRelation节点，new一个对象
			zteNeighborhoodRelationship=new ZTENeighborhoodRelationship();
		}
		preTag = qName;
	}
	
	
	/**
	 * 扫描到标签中内容，进入该方法
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		
		if (zteNeighborhoodRelationship != null) { //如果不等于null，说明还在ExternalEUtranCellFDD这一整层节点中
          String data = stringBuilder.append(ch, start, length).toString();
          if ("SubNetwork".equals(preTag)) {
              zteNeighborhoodRelationship.sc_SUBNETWORK=data;
          }
          if ("MEID".equals(preTag)) {
              zteNeighborhoodRelationship.sc_MEID=data;
          }
          if ("ENBFunctionFDD".equals(preTag)) {
              zteNeighborhoodRelationship.sc_ENBFUNCTIONFDD=data;
          }
          if ("EUtranCellFDD".equals(preTag)) {
              zteNeighborhoodRelationship.sc_EUTRANCELLFDD=data;
          }
          if ("refExternalEUtranCellFDD".equals(preTag)) {
          		String[] refSplit = data.split(",", -1);
          		String[] split;
				if(refSplit.length == 4){
					split = refSplit[0].split("=", -1);
					if(split.length==2)
					{
						zteNeighborhoodRelationship.nc_SUBNETWORK = split[1];
					}
					split = refSplit[1].split("=", -1);
					if(split.length==2)
					{
						zteNeighborhoodRelationship.nc_MEID = split[1];
					}
					split = refSplit[2].split("=", -1);
					if(split.length==2)
					{
						zteNeighborhoodRelationship.nc_ENBFUNCTIONFDD = split[1];
					}
					split = refSplit[3].split("=", -1);
					if(split.length==2)
					{
						zteNeighborhoodRelationship.nc_EXTERNALEUTRANCELLFDD = split[1];
					}
					
					
				}
          	
          	}
       
		}
	}

	
	
	/**
	 * 扫描到结束标签，进入该方法
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
		//这里要加判断，主要是因为EUtranRelation这一层父节点中有一个子节点也叫EUtranRelation
		if(qName.equals("EUtranRelation")&&!"EUtranRelation".equals(preTag))
		{
			zteRelationshiplist.add(zteNeighborhoodRelationship);
			zteNeighborhoodRelationship=null;
		}
		
		preTag=null;
	}
	

	/**.
	 * 结束
	 */
	@Override
	public void endDocument() throws SAXException {
		
	}
}