package com.conf.parse.analysis;

import com.conf.parse.InitTestEnvironment;
import com.conf.parse.main.DataAnalysis;


public abstract class AnalysisTest {

    public void run(String dateTime, String city, AnalysisType anaType) {

        InitTestEnvironment.setDerbyHome();
        String str = "-dateTime " + dateTime + " " //
                + "-city " + city + " "            //
                + "-analysisTypes " + anaType + " "//
                + "-devTestMode true";
        DataAnalysis.main(str.split(" "));
    }

}
