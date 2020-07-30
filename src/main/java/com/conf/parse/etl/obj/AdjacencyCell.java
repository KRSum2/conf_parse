package com.conf.parse.etl.obj;


/**
 * 邻接小区
 * @author 黄世良
 *
 */
public class AdjacencyCell {
	
	public String nc_ENODEBID; //邻接小区基站号
	public String nc_CELLID; //邻接小区编号
	public String nc_PHYCELLID;  //邻接小区PCI
	public String nc_DLEARFCN;  //邻接小区频点
	public String nc_MCC;
	public String nc_MNC;
	public String nc_TAC;
	
	@Override
	public String toString() {
		return "AdjacencyCell [nc_ENODEBID=" + nc_ENODEBID + ", nc_CELLID=" + nc_CELLID + ", nc_PHYCELLID="
				+ nc_PHYCELLID + ", nc_DLEARFCN=" + nc_DLEARFCN + ", nc_MCC=" + nc_MCC + ", nc_MNC=" + nc_MNC
				+ ", nc_TAC=" + nc_TAC + "]";
	}
	
}
