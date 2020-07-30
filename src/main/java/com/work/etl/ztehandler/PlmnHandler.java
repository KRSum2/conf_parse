package com.work.etl.ztehandler;

import com.conf.parse.etl.zteobjects.ZTEPlmn;
import com.work.etl.zteobjects.ZTEPlmn;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

public class PlmnHandler extends DefaultHandler{
	public String preTag;//上一个标签名
	public ArrayList<ZTEPlmn> ztePlmnList;
	public ZTEPlmn ztePlmn;
	public StringBuilder stringBuilder;
	
	public PlmnHandler(ArrayList<ZTEPlmn> ztePlmnList) {
		this.ztePlmnList = ztePlmnList;
	}

	/**
	 * 开始读取xml文档
	 */
	@Override
	public void startDocument() throws SAXException {
		stringBuilder = new StringBuilder();
	}
	
	
	/**
	 * 扫描到开始标签，进入该方法
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		
		stringBuilder.setLength(0);
		/*
		 *  遇到ExternalEUtranCellFDD节点，new一个对象，
		 *  注意有限制，如果没有不是<Plmn id="188">这种有id的不会new对象，
		 *  这样就防止在遇到Plmn里的Plmn子节点时又new 对象
		 */
		if ("Plmn".equals(qName)&&attributes.getQName(0)!=null) {			
			ztePlmn = new ZTEPlmn();
		}
		preTag = qName;
	}
	
	
	/**
	 * 扫描到标签中内容，进入该方法
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		
		if (ztePlmn != null && ztePlmn != null) {  //如果不等于null，说明还在Plmn这一整层节点中
			String data = stringBuilder.append(ch, start, length).toString();
            if ("SubNetwork".equals(preTag)) {
            	ztePlmn.sc_SUBNETWORK1 = data;
            }
            if ("MEID".equals(preTag)) {
            	ztePlmn.sc_MEID1 = data;
            }
            if ("Operator".equals(preTag)) {
            	ztePlmn.sc_OPERATOR = data;               
            }
            //这里要加判断，主要是因为Plmn这一层父节点中有一个子节点也叫Plmn
            if ("Plmn".equals(preTag)&&"Plmn".equals(preTag)) {
            	ztePlmn.sc_PLMN = data;
            }
            if("mcc".equals(preTag)){
            	ztePlmn.sc_MCC = data;
            }
            if("mnc".equals(preTag)){
            	ztePlmn.sc_MNC = data;
            }
        }
	}

	
	/**
	 * 扫描到结束标签，进入该方法
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
		//这里要加判断，主要是因为Plmn这一层父节点中有一个子节点也叫Plmn
		if(qName.equals("Plmn")&&!"Plmn".equals(preTag))
		{
			//System.out.println(zteAdjacencyCell);
			ztePlmnList.add(ztePlmn);
			ztePlmn=null; 
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
