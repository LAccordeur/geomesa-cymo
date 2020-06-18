/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client;

import com.rogerguo.common.CommandLineDataStore;
import com.rogerguo.cymo.entity.SpatialRange;
import com.rogerguo.cymo.entity.TimeRange;
import com.rogerguo.data.CommonData;
import com.rogerguo.test.VerifyUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.*;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.factory.Hints;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.*;

import static com.rogerguo.test.VerifyUtil.fromDateToTimestamp;

//import org.geotools.factory.Hints;

public abstract class GeoMesaClient implements Runnable {

    private final Map<String, String> params;
    private final CommonData data;
    private final boolean cleanup;
    private final boolean readOnly;

    public GeoMesaClient(String[] args, Param[] parameters, CommonData data) throws ParseException {
        this(args, parameters, data, false);
    }

    public GeoMesaClient(String[] args, Param[] parameters, CommonData data, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        cleanup = command.hasOption("cleanup");
        this.data = data;
        this.readOnly = readOnly;
        initializeFromOptions(command);
    }

    public Options createOptions(Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    public void initializeFromOptions(CommandLine command) {
    }

    @Override
    public void run() {
        DataStore datastore = null;
        try {
            datastore = createDataStore(params); //创建一个HBaseDataStore实例
            if (readOnly) {
                ensureSchema(datastore, data);
            } else {
                SimpleFeatureType sft = getSimpleFeatureType(data);
                createSchema(datastore, sft);
                /*List<SimpleFeature> features = getTestFeatures(data);
                writeFeatures(datastore, sft, features);*/
                writeTestFeatures(data, this, sft, datastore);
            }

            List<Query> queries = getTestQueries(data);

            queryFeatures(datastore, queries);
        } catch (Exception e) {
            throw new RuntimeException("Error running quickstart:", e);
        } finally {
            cleanup(datastore, data.getTypeName(), cleanup);
        }
        System.out.println("Done");
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Loading datastore");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void ensureSchema(DataStore datastore, CommonData data) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
        if (sft == null) {
            throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. " +
                                            "Please run the associated QuickStart to generate the test data.");
        }
    }

    public SimpleFeatureType getSimpleFeatureType(CommonData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public List<SimpleFeature> getTestFeatures(CommonData data) {
        System.out.println("Generating test data");
        List<SimpleFeature> features = data.getTestData();
        System.out.println();
        return features;
    }

    public void writeTestFeatures(CommonData data, GeoMesaClient client, SimpleFeatureType sft, DataStore dataStore) {
        System.out.println("Writing test data");
        data.writeTestData(client, dataStore, sft);
        System.out.println();
    }

    public List<Query> getTestQueries(CommonData data) {
        return data.getTestQueries();
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            System.out.println("Writing test data");
            long startTime = System.currentTimeMillis();
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                     ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                     toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
            long stopTime = System.currentTimeMillis();
            System.out.println("Wrote " + features.size() + " features and consumed " + (stopTime - startTime) / 1000 + " s");
            System.out.println();
        }
    }

    public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
        for (Query query : queries) {

            List<String> resultString = new ArrayList<>();

            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            // submit the query, and get back an iterator over matching features
            // use try-with-resources to ensure the reader is closed
            long startTime = System.currentTimeMillis();
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                int n = 0;

                int realCount = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                        Date date = (Date) feature.getAttribute("dtg");
                        Point geom = (Point) feature.getAttribute("geom");

                        //String[] geomItems = geom.split();
                        System.out.println(date);
                        System.out.println(geom.getX());
                        System.out.println(geom.getY());

                    } else if (n == 10) {
                        System.out.println("...");
                    }


                    Date date = (Date) feature.getAttribute("dtg");
                    Point geom = (Point) feature.getAttribute("geom");
                    String seqID = (String) feature.getAttribute("seq_id");
                    StringBuffer stringBuffer = new StringBuffer();
                    resultString.add(stringBuffer.append(seqID).append(geom.getX()).append(geom.getY()).append(date.getTime()).toString());

                    SpatialRange longitudeRange = new SpatialRange(-73.968171, -73.965171);
                    SpatialRange latitudeRange = new SpatialRange(40.762236,40.766236);
                    TimeRange timeRange = new TimeRange(fromDateToTimestamp("2010-01-01 00:00:00"), fromDateToTimestamp("2010-01-01 15:25:00"));
                    if (SpatialRange.isInRange(geom.getX(), longitudeRange) && SpatialRange.isInRange(geom.getY(), latitudeRange) && TimeRange.isInRange(date.getTime(), timeRange)) {
                        realCount++;
                    }
                }

                //VerifyUtil.verify(resultString);

                System.out.println();
                System.out.println("real count: " + realCount);
                System.out.println("Returned " + n + " total features");
                System.out.println();
            }
            long stopTime = System.currentTimeMillis();
            System.out.println("This query consumes " + (stopTime - startTime) + "ms");
        }
    }

    public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
        if (datastore != null) {
            try {
                if (cleanup) {
                    System.out.println("Cleaning up test data");
                    if (datastore instanceof GeoMesaDataStore) {
                        ((GeoMesaDataStore) datastore).delete();
                    } else {
                        ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                        datastore.removeSchema(typeName);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception cleaning up test data: " + e.toString());
            } finally {
                // make sure that we dispose of the datastore when we're done with it
                datastore.dispose();
            }
        }
    }
}
