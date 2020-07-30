package com.work.etl.hwobjects;

public class BTS3900EUTRANEXTERNALCELL {
	public String nc_ENODEBID; //邻接小区基站号
	public String nc_CELLID; //邻接小区编号
	public String nc_MCC;
	public String nc_MNC;
	public String nc_TAC;
	@Override
	public String toString() {
		return "BTS3900EUTRANEXTERNALCELL [nc_ENODEBID=" + nc_ENODEBID + ", nc_CELLID=" + nc_CELLID + ", nc_MCC="
				+ nc_MCC + ", nc_MNC=" + nc_MNC + ", nc_TAC=" + nc_TAC + "]";
	}
	
	
}
