package com.conf.parse.etl;

import com.conf.parse.etl.parser.EriceXmlETL;
import com.conf.parse.etl.parser.HwXmlETL;
import com.conf.parse.etl.parser.ZteSaxXmlETL;

public enum ETLType {
    Zte_Sax_Xml(ZteSaxXmlETL.class),
    Hw_Sax_Xml(HwXmlETL.class),
    Erice_Sax_Xml(EriceXmlETL.class),
    ;

    private Class<? extends ETL> etlClass;
    private final boolean runByDay; // 按天调度



    private ETLType(Class<? extends ETL> etlClass) {
        this(etlClass, true);
    }

    private ETLType(Class<? extends ETL> etlClass, boolean runByDay) {
        this.etlClass = etlClass;
        this.runByDay = runByDay;
    }

    public boolean runByDay() {
        return runByDay;
    }

    public ETL newEtlInstance() {
        try {
            return etlClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Instance exception: " + etlClass.getName(), e);
        }
    }

}
