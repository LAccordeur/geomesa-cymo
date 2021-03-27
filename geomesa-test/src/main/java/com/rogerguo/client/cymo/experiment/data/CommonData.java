/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package com.rogerguo.client.cymo.experiment.data;

import com.rogerguo.client.cymo.experiment.test.GeoMesaClient;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.util.List;

public interface CommonData {

    String getTypeName();
    SimpleFeatureType getSimpleFeatureType();
    List<SimpleFeature> getTestData();
    List<Query> getTestQueries();
    Filter getSubsetFilter();
    void writeTestData(GeoMesaClient client, DataStore datastore, SimpleFeatureType sft);

}
