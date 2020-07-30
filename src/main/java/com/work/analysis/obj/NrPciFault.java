package com.work.analysis.obj;

public class NrPciFault {
    public String sc_enodebid;
    public String sc_enodebname;
    public int sc_cellid;
    public String sc_cellname;
    public int sc_pci;
    public int sc_fcn;
    public String sc_city;
    public String sc_vendor;
    public String problem;
    public String nc_enodebid1;
    public String nc_enodebname1;
    public String nc_cellid1;
    public int nc_pci1;
    public int nc_fcn1;
    public String nc_city1;
    public String nc_vendor1;
    public String nc_enodebid2;
    public String nc_enodebname2;
    public String nc_cellid2;
    public String nc_pci2;
    public int nc_fcn2;
    public String nc_city2;
    public String nc_vendor2;
    public String runningDate;


    @Override
    public String toString() {
        return "NrPciFault{" +
                "sc_enodebid='" + sc_enodebid + '\'' +
                ", sc_enodebname='" + sc_enodebname + '\'' +
                ", sc_cellid=" + sc_cellid +
                ", sc_cellname='" + sc_cellname + '\'' +
                ", sc_pci=" + sc_pci +
                ", sc_fcn=" + sc_fcn +
                ", sc_city='" + sc_city + '\'' +
                ", sc_vendor='" + sc_vendor + '\'' +
                ", problem='" + problem + '\'' +
                ", nc_enodebid1='" + nc_enodebid1 + '\'' +
                ", nc_enodebname1='" + nc_enodebname1 + '\'' +
                ", nc_cellid1='" + nc_cellid1 + '\'' +
                ", nc_pci1=" + nc_pci1 +
                ", nc_fcn1=" + nc_fcn1 +
                ", nc_city1='" + nc_city1 + '\'' +
                ", nc_vendor1='" + nc_vendor1 + '\'' +
                ", nc_enodebid2='" + nc_enodebid2 + '\'' +
                ", nc_enodebname2='" + nc_enodebname2 + '\'' +
                ", nc_cellid2='" + nc_cellid2 + '\'' +
                ", nc_pci2='" + nc_pci2 + '\'' +
                ", nc_fcn2=" + nc_fcn2 +
                ", nc_city2='" + nc_city2 + '\'' +
                ", nc_vendor2='" + nc_vendor2 + '\'' +
                ", runningDate='" + runningDate + '\'' +
                '}';
    }
}
