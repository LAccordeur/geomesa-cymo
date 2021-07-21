/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client.cymo.experiment.test;

import com.rogerguo.client.cymo.experiment.data.CommonData;
import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClient extends GeoMesaClient {



    public HBaseClient(String[] args, CommonData data, boolean readOnly, String logFilename) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), data, readOnly, logFilename);
    }

    public static void main(String[] args) {
        try {
//            String logFilename = "G:\\DataSet\\production-next-passenger\\response-time-log\\production.double.04.05.cymo.ztxy.test.48.24.min2.3.csv";
//            String queryFilename = "G:\\DataSet\\production-next-passenger\\workload_1_next_passenger_04_05_sample";

//            String logFilename = "G:\\DataSet\\production-v4\\response-time-log\\production.geomesa.z3.day.05_05.csv";
//            String queryFilename = "G:\\DataSet\\production-v4\\workload_1_next_passenger_05_05_sample";

            String logFilename = "G:\\DataSet\\synthetic-test-v2\\response-time-log\\synthetic.cymo.fixed-time.384.192.test.csv";
            String queryFilename = "G:\\DataSet\\synthetic-test-v2\\test";

            String dataFilename = "E:\\Projects\\idea\\geomesa-cymo\\geomesa-test\\src\\main\\resources\\dataset\\trip_data_1_pickup.csv";
            CommonData data = new NYCTaxiFormattedDataTestSynthetic(queryFilename, dataFilename);
            //CommonData data = new NYCTaxiFormattedDataTestGeoMesaZ3Day(queryFilename, dataFilename);
            //CommonData data = new NYCTaxiFormattedDataTestGeoMesaZ3DayInsertion(null, dataFilename);
            //CommonData data = new NYCTaxiFormattedDataTestGeoMesaZ3Synthetic(queryFilename, dataFilename);
            HBaseClient client = new HBaseClient(args, data, true, logFilename);
            client.myExecute();
            //client.myExecute();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
