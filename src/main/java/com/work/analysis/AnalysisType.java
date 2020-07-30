package com.work.analysis;


import com.conf.parse.analysis.DataImportMySQL.DataImportAnalysis;
import com.conf.parse.analysis.LnnrwholeAnalysis.LNNRWholeAnalysis;

public enum AnalysisType {
    LNNR_Whole(LNNRWholeAnalysis.class),
    Lte_NRO_Fault_Count(LteNROFaultCount.class),
    Data_Import(DataImportAnalysis.class),
    ;


    private final Class<? extends Analysis> aClass;
    private final boolean runByDay; // 按天调度

    private AnalysisType(Class<? extends Analysis> aClass) {
        this(aClass, true);
    }

    private AnalysisType(Class<? extends Analysis> aClass, boolean runByDay) {
        this.aClass = aClass;
        this.runByDay = runByDay;
    }

    public boolean runByDay() {
        return runByDay;
    }

    public Analysis newAnalysisInstance() {
        try {
            return aClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Instance exception: " + aClass.getName(), e);
        }
    }
}
