package com.work.analysis.obj;

public class NrArgsFault {
    public String sc_enodebid;
    public String sc_enodebname;
    public int sc_cellid;
    public String sc_cellname;
    public int sc_pci;
    public int sc_fcn;
    public String sc_city;
    public String sc_vendor;
    public String problem;
    public int recommend_pci;
    public String wrong_configure;
    public String runningDate;

    @Override
    public String toString() {
        return "NrArgsFault{" +
                "sc_enodebid='" + sc_enodebid + '\'' +
                ", sc_enodebname='" + sc_enodebname + '\'' +
                ", sc_cellid=" + sc_cellid +
                ", sc_cellname='" + sc_cellname + '\'' +
                ", sc_pci=" + sc_pci +
                ", sc_fcn=" + sc_fcn +
                ", sc_city='" + sc_city + '\'' +
                ", sc_vendor='" + sc_vendor + '\'' +
                ", problem='" + problem + '\'' +
                ", recommend_pci=" + recommend_pci +
                ", wrong_configure='" + wrong_configure + '\'' +
                ", runningDate='" + runningDate + '\'' +
                '}';
    }
}
