/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client.workload;

import com.rogerguo.client.GeoMesaClient;
import com.rogerguo.data.workload.NYCTaxiFormattedDataCymoHilbertWorkloadThree;
import com.rogerguo.data.workload.NYCTaxiFormattedDataCymoT1X7Y7WorkloadOne;
import com.rogerguo.data.workload.NYCTaxiFormattedDataCymoT1X7Y7WorkloadTwo;
import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClientWorkloadCymoT1X7Y7 extends GeoMesaClient {


    public HBaseClientWorkloadCymoT1X7Y7(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new NYCTaxiFormattedDataCymoT1X7Y7WorkloadTwo());
    }

    public static void main(String[] args) {
        try {
            new HBaseClientWorkloadCymoT1X7Y7(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
