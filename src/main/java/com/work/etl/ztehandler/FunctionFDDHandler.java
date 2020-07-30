package com.work.etl.ztehandler;

import com.conf.parse.etl.obj.Function;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

/**
 * 基站信息
 * @author 赖书恒
 *
 */
public class FunctionFDDHandler extends DefaultHandler {
	public String preTag;//上一个标签名
	public ArrayList<Function> functionList;
	public Function function;//中兴的基站对象
	public StringBuilder stringBuilder;
	
	public FunctionFDDHandler(ArrayList<Function> functionList) {
		this.functionList = functionList;
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
		 *  遇到ENBFunctionFDD节点，new一个对象，
		 *  注意有限制，如果没有不是<EUtranRelation id="533366">这种有id的不会new对象，
		 *  这样就防止在遇到ENBFunctionFDD里的ENBFunctionFDD子节点时又new 对象
		 */
		if ("ENBFunctionFDD".equals(qName)&&attributes.getQName(0)!=null) {
			// 遇到EUtranRelation节点，new一个对象
			function=new Function();
		}
		preTag = qName;
	}
	
	
	/**
	 * 扫描到标签中内容，进入该方法
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		
		if (function != null) { //如果不等于null，说明还在ExternalEUtranCellFDD这一整层节点中
			String data = stringBuilder.append(ch, start, length).toString();
			if ("eNBId".equals(preTag)) {
	        	function.sc_ENODEBID = data;
	        }
	        if ("enbName".equals(preTag)) {
	        	function.sc_ENODEBFUNCTIONNAME = data;
	        }
          /*if ("eNBId".equals(preTag)) {
        	  stringBuilder.append(ch, start, length);
          }
          if ("enbName".equals(preTag)) {
        	  stringBuilder.append(ch, start, length);
          }*/
         
      }
	}

	
	
	/**
	 * 扫描到结束标签，进入该方法
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
		//这里要加判断，主要是因为ENBFunctionFDD这一层父节点中有一个子节点也叫ENBFunctionFDD
		if(qName.equals("ENBFunctionFDD")&&!"ENBFunctionFDD".equals(preTag))
		{
	        
			//主小区地市
			function.sc_LOCATIONNAME = " ";
			//主小区厂家
			function.sc_VENDORNAME = "ZTE";
			functionList.add(function);
			
			/*System.out.println("-----------------------" + functionList.size() + "----------------------------------------");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			function=null;
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
			//PmAnalysisCommon.pmFileWrite(fs,"/user/noce/DATA/PUBLIC/NOCE/ETL/ETL_CONF_ANALYZ_FUNCTION/ZTE_FUNCTION.CSV",functionList,false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
