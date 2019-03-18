package com.intuit.reportgen;

/**
 * User: jeyanthan
 * Date: 2019-03-15
 * Description:
 *      Linker to invoke the scala Report Driver from the fat jar submitted by spark.
 */
public class ReportGen {

    public static void main(String[] args){

        ReportDriver hw = new ReportDriver();
        hw.main(args);

    }
}
