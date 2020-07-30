package com.conf.parse.etl;

import com.conf.parse.InitTestEnvironment;
import com.conf.parse.main.DataETL;


public abstract class EtlTest {

    public void run(String dateTime, String city, ETLType etlType) {

        InitTestEnvironment.setDerbyHome();
        String str = "-dateTime " + dateTime + " " //
                + "-city " + city + " "   //
                + "-etlTypes " + etlType + " " //
                + "-devTestMode true";
        DataETL.main(str.split(" "));
    }

}
