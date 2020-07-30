package com.conf.parse.etl.obj;

/**
 * 基站信息
 * @author 黄世良
 *
 */
public class Function {
	
	public String sc_ENODEBID; //主小区基站号
	public String sc_ENODEBFUNCTIONNAME; //主小区基站名
	public String sc_LOCATIONNAME; //主小区地市
	public String sc_VENDORNAME;  //主小区厂家
	
	
	@Override
	public String toString() {
		return "Function [sc_ENODEBID=" + sc_ENODEBID + ", sc_ENODEBFUNCTIONNAME=" + sc_ENODEBFUNCTIONNAME
				+ ", sc_LOCATIONNAME=" + sc_LOCATIONNAME + ", sc_VENDORNAME=" + sc_VENDORNAME + "]";
	}

	
	
}
