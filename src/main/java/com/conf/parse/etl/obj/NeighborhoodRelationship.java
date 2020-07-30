package com.conf.parse.etl.obj;


/**
 * 邻区关系
 * @author 黄世良
 *
 */
public class NeighborhoodRelationship {
	
	public String sc_ENODEBID; //主小区基站号
	public String sc_LOCALCELLID; //主小区小区编号
	
	public String nc_ENODEBID; //邻接小区基站号
	public String nc_CELLID; //邻接小区编号

	
	@Override
	public String toString() {
		return "NeighborhoodRelationship [sc_ENODEBID=" + sc_ENODEBID + ", sc_LOCALCELLID=" + sc_LOCALCELLID
				+ ", nc_ENODEBID=" + nc_ENODEBID + ", nc_CELLID=" + nc_CELLID + "]";
	}
	
	
	
}
