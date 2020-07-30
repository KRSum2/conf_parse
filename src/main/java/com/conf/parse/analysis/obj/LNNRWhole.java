package com.conf.parse.analysis.obj;


/**
 * 总表
 *
 * @author 黄世良
 */
public class LNNRWhole {
    public String sc_ENODEBID; //主小区基站号
    public String sc_ENODEBFUNCTIONNAME; //主小区基站名
    public String sc_LOCALCELLID; //主小区小区编号
    public String sc_CELLNAME;  //主小区小区名称
    public String sc_FDDTDDIND; //主小区制式
    public String sc_PHYCELLID;  //主小区PCI
    public String sc_DLEARFCN;  //主小区频点
    public String sc_LOCATIONNAME; //主小区地市
    public String sc_VENDORNAME;  //主小区厂家
    public String sc_MCC;
    public String sc_MNC;
    public String sc_TAC;

    public String nc_ENODEBID; //邻接小区基站号
    public String nc_ENODEBFUNCTIONNAME; //邻接小区基站名
    public String nc_CELLID; //邻接小区编号
    public String nc_PHYCELLID;  //邻接小区PCI
    public String nc_DLEARFCN;  //邻接小区频点
    public String nc_LOCATIONNAME; //主小区地市
    public String nc_VENDORNAME;  //主小区厂家
    public String nc_MCC;
    public String nc_MNC;
    public String nc_TAC;

    public String sc_nc_NR;   //同频或异频


    @Override
    public String toString() {
        return "LNNRWhole{" +
                "sc_ENODEBID='" + sc_ENODEBID + '\'' +
                ", sc_ENODEBFUNCTIONNAME='" + sc_ENODEBFUNCTIONNAME + '\'' +
                ", sc_LOCALCELLID='" + sc_LOCALCELLID + '\'' +
                ", sc_CELLNAME='" + sc_CELLNAME + '\'' +
                ", sc_FDDTDDIND='" + sc_FDDTDDIND + '\'' +
                ", sc_PHYCELLID='" + sc_PHYCELLID + '\'' +
                ", sc_DLEARFCN='" + sc_DLEARFCN + '\'' +
                ", sc_LOCATIONNAME='" + sc_LOCATIONNAME + '\'' +
                ", sc_VENDORNAME='" + sc_VENDORNAME + '\'' +
                ", sc_MCC='" + sc_MCC + '\'' +
                ", sc_MNC='" + sc_MNC + '\'' +
                ", sc_TAC='" + sc_TAC + '\'' +
                ", nc_ENODEBID='" + nc_ENODEBID + '\'' +
                ", nc_ENODEBFUNCTIONNAME='" + nc_ENODEBFUNCTIONNAME + '\'' +
                ", nc_CELLID='" + nc_CELLID + '\'' +
                ", nc_PHYCELLID='" + nc_PHYCELLID + '\'' +
                ", nc_DLEARFCN='" + nc_DLEARFCN + '\'' +
                ", nc_LOCATIONNAME='" + nc_LOCATIONNAME + '\'' +
                ", nc_VENDORNAME='" + nc_VENDORNAME + '\'' +
                ", sc_nc_NR='" + sc_nc_NR + '\'' +
                ", nc_MCC='" + nc_MCC + '\'' +
                ", nc_MNC='" + nc_MNC + '\'' +
                ", nc_TAC='" + nc_TAC + '\'' +
                '}';
    }
}
