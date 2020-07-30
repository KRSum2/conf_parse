package com.work.etl.obj;

/**
 * 本地小区信息
 *
 * @author 黄世良
 */
public class LocalCell {

    public String sc_ENODEBID; //主小区基站号
    public String sc_CELLID;//主小区小区标识号
    public String sc_LOCALCELLID; //主小区小区编号
    public String sc_CELLNAME;  //主小区小区名称
    public String sc_FDDTDDIND; //主小区制式
    public String sc_PHYCELLID;  //主小区PCI
    public String sc_DLEARFCN;  //主小区频点
    public String sc_MCC;
    public String sc_MNC;
    public String sc_TAC;


    @Override
    public String toString() {
        return "LocalCell{" +
                "sc_ENODEBID='" + sc_ENODEBID + '\'' +
                ", sc_LOCALCELLID='" + sc_LOCALCELLID + '\'' +
                ", sc_CELLID='" + sc_CELLID + '\'' +
                ", sc_CELLNAME='" + sc_CELLNAME + '\'' +
                ", sc_FDDTDDIND='" + sc_FDDTDDIND + '\'' +
                ", sc_PHYCELLID='" + sc_PHYCELLID + '\'' +
                ", sc_DLEARFCN='" + sc_DLEARFCN + '\'' +
                ", sc_MCC='" + sc_MCC + '\'' +
                ", sc_MNC='" + sc_MNC + '\'' +
                ", sc_TAC='" + sc_TAC + '\'' +
                '}';
    }
}
