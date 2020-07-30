package com.conf.parse.etl.zteobjects;

/**
 * 中兴邻接小区
 * @author 赖书恒
 *
 */
public class ZTEAdjacencyCell {

	public String nc_ENODEBID; //邻接小区基站号
	public String nc_CELLID; //邻接小区编号
	public String nc_PHYCELLID;  //邻接小区PCI
	public String nc_DLEARFCN;  //邻接小区频点
	//关联所需字段
	public String nc_SUBNETWORK;
	public String nc_MEID;
	public String nc_ENBFUNCTIONFDD; //邻接小区基站号
	public String nc_EXTERNALEUTRANCELLFDD;
	@Override
	public String toString() {
		return "ZTEAdjacencyCell [nc_ENODEBID=" + nc_ENODEBID + ", nc_CELLID=" + nc_CELLID + ", nc_PHYCELLID="
				+ nc_PHYCELLID + ", nc_DLEARFCN=" + nc_DLEARFCN + ", nc_SUBNETWORK=" + nc_SUBNETWORK + ", nc_MEID="
				+ nc_MEID + ", nc_ENBFUNCTIONFDD=" + nc_ENBFUNCTIONFDD + ", nc_EXTERNALEUTRANCELLFDD="
				+ nc_EXTERNALEUTRANCELLFDD + "]";
	}
	
}
