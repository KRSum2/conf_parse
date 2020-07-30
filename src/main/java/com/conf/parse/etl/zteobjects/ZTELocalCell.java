package com.conf.parse.etl.zteobjects;

/**
 * 中兴主小区
 * @author 赖书恒
 *
 */
public class ZTELocalCell {
	
	public String sc_ENODEBID; //主小区基站号(关联所需字段)
	public String sc_LOCALCELLID; //主小区小区编号
	public String sc_CELLNAME;  //主小区小区名称
	public String sc_FDDTDDIND; //主小区制式
	public String sc_PHYCELLID;  //主小区PCI
	public String sc_DLEARFCN;  //主小区频点
	//关联关系所需字段
	public String sc_SUBNETWORK;
	public String sc_MEID;
	public String sc_EUTRANCELLFDD;
	//第二关联（与关联关系无关）
	public String sc_SUBNETWORK1;
	public String sc_MEID1;
	public String sc_OPERATOR;
	public String sc_PLMN;
	public String sc_TAC;
	@Override
	public String toString() {
		return "ZTELocalCell [sc_ENODEBID=" + sc_ENODEBID + ", sc_LOCALCELLID="
				+ sc_LOCALCELLID + ", sc_CELLNAME=" + sc_CELLNAME + ", sc_FDDTDDIND=" + sc_FDDTDDIND + ", sc_PHYCELLID="
				+ sc_PHYCELLID + ", sc_DLEARFCN=" + sc_DLEARFCN + ", sc_SUBNETWORK=" + sc_SUBNETWORK + ", sc_MEID="
				+ sc_MEID + ", sc_EUTRANCELLFDD=" + sc_EUTRANCELLFDD + ", sc_SUBNETWORK1=" + sc_SUBNETWORK1
				+ ", sc_MEID1=" + sc_MEID1 + ", sc_OPERATOR=" + sc_OPERATOR + ", sc_PLMN=" + sc_PLMN + ", sc_TAC="
				+ sc_TAC + "]";
	}
		
}
