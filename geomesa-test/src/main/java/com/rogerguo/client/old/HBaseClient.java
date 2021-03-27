/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client.old;

import com.rogerguo.data.exp2.workload1.NYCTaxiFormattedDataExp2CymoWorkload1SinglePattern;
import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClient extends GeoMesaClient {


    public HBaseClient(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new NYCTaxiFormattedDataExp2CymoWorkload1SinglePattern());
    }

    public static void main(String[] args) {
        try {
            new HBaseClient(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
