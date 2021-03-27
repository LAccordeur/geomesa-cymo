/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client.old;

import com.rogerguo.data.workload.*;
import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClientForZ3Index extends GeoMesaClient {


    public HBaseClientForZ3Index(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new NYCTaxiFormattedDataZ3WorkloadThree());
    }

    public static void main(String[] args) {
        try {
            new HBaseClientForZ3Index(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
