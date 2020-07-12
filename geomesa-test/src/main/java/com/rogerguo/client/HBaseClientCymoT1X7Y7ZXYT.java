/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client;

import com.rogerguo.data.NYCTaxiFormattedDataCymoT1X7Y7AndZXYTForHybridWeek;
import com.rogerguo.data.NYCTaxiFormattedDataCymoZTXYAndZXYTForHybridWeek;
import org.apache.commons.cli.ParseException;
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;

public class HBaseClientCymoT1X7Y7ZXYT extends GeoMesaClient {


    public HBaseClientCymoT1X7Y7ZXYT(String[] args) throws ParseException {
        super(args, new HBaseDataStoreFactory().getParametersInfo(), new NYCTaxiFormattedDataCymoT1X7Y7AndZXYTForHybridWeek());
    }

    public static void main(String[] args) {
        try {
            new HBaseClientCymoT1X7Y7ZXYT(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
