package com.conf.parse.etl.zteobjects;

public class ZTENeighborhoodRelationship {

	//关联所需字段
	public String sc_SUBNETWORK;
	public String sc_MEID;
	public String sc_ENBFUNCTIONFDD; //主小区基站号
	public String sc_EUTRANCELLFDD;
	
	//由refExternalEUtranCellFDD标签分割得到
	//关联所需字段
	public String nc_SUBNETWORK;
	public String nc_MEID;
	public String nc_ENBFUNCTIONFDD; //邻接小区基站号
	public String nc_EXTERNALEUTRANCELLFDD;
	@Override
	public String toString() {
		return "ZTENeighborhoodRelationship [sc_SUBNETWORK=" + sc_SUBNETWORK + ", sc_MEID=" + sc_MEID
				+ ", sc_ENBFUNCTIONFDD=" + sc_ENBFUNCTIONFDD + ", sc_EUTRANCELLFDD=" + sc_EUTRANCELLFDD
				+ ", nc_SUBNETWORK=" + nc_SUBNETWORK + ", nc_MEID=" + nc_MEID + ", nc_ENBFUNCTIONFDD="
				+ nc_ENBFUNCTIONFDD + ", nc_EXTERNALEUTRANCELLFDD=" + nc_EXTERNALEUTRANCELLFDD + "]";
	}
	
	
	
}
