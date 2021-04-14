package com.rogerguo.client.cymo.experiment.test;


import com.rogerguo.client.cymo.IndexingSchemeDecider;
import com.rogerguo.client.cymo.experiment.data.CommonData;
import com.rogerguo.cymo.config.VirtualLayerConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

//import org.geotools.factory.Hints;

/**
 * @Description
 * @Date 2019/4/29 15:25
 * @Created by rogerguo
 */
public class NYCTaxiFormattedDataTestSynthetic implements CommonData {
    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;

    /*@Override
    public String getTypeName() {
        return "nyc-taxi-data-month1-precision18";
    }*/

    @Override
    public String getTypeName() {
        //return "nyc-taxi-data-month1-xzprecision20-geomprecision7";
        return VirtualLayerConfiguration.configMap.get("FEATURE_TYPE_DATA");
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            // list the attributes that constitute the feature type
            // this is a reduced set of the attributes from GDELT 2.0
            StringBuilder attributes = new StringBuilder();
            attributes.append("seq_id:String,");
            attributes.append("medallion:String,");
            attributes.append("trip_time_in_secs:Double,");
            attributes.append("trip_distance:Double,");
            attributes.append("dtg:Date,");
            attributes.append("*geom:Point:srid=4326"); // the "*" denotes the default geometry (used for indexing)

            // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
            // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());

            // use the user-data (hints) to specify which date field to use for primary indexing
            // if not specified, the first date attribute (if any) will be used
            // could also use ':default=true' in the attribute specification string
            sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");

            // higher precision
            //sft.getUserData().put("geomesa.xz.precision", "20");
            //sft.getUserData().put("geomesa.z3.interval", "month");
            //sft.getDescriptor("geom").getUserData().put("precision", "7");

            sft.getUserData().put("geomesa.indices.enabled", "cymo");


        }
        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the bundled t-drive CSV
            URL input = getClass().getClassLoader().getResource("dataset/trip_data_1_pickup.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource trip_data_1_pickup.csv");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
                for (CSVRecord record : parser) {
                    // pull out the fields corresponding to our simple feature attributes
                    builder.set("seq_id", record.get(0));
                    builder.set("medallion", record.get(1));
                    builder.set("trip_time_in_secs", record.get(3));
                    builder.set("trip_distance", record.get(4));
                    // some dates are converted implicitly, so we can set them as strings
                    // however, the date format here isn't one that is converted, so we parse it into a java.util.Date
                    //System.out.println(record.get(0));
                    builder.set("dtg", Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)));

                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering
                    double longitude = Double.parseDouble(record.get(5));
                    double latitude = Double.parseDouble(record.get(6));
                    builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(record.get(0)));
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading taxi data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }

        return features;
    }

    @Override
    public void writeTestData(GeoMesaClient client, DataStore datastore, SimpleFeatureType sft) {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the bundled t-drive CSV
            URL input = getClass().getClassLoader().getResource("dataset/trip_data_1_pickup.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource trip_data_1_pickup.csv");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            int count = 0;
            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
                for (CSVRecord record : parser) {
                    // pull out the fields corresponding to our simple feature attributes
                    builder.set("seq_id", record.get(0));
                    builder.set("medallion", record.get(1));
                    builder.set("trip_time_in_secs", record.get(3));
                    builder.set("trip_distance", record.get(4));
                    // some dates are converted implicitly, so we can set them as strings
                    // however, the date format here isn't one that is converted, so we parse it into a java.util.Date
                    //System.out.println(record.get(0));
                    builder.set("dtg", Date.from(LocalDateTime.parse(record.get(2), dateFormat).toInstant(ZoneOffset.UTC)));

                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering
                    double longitude = Double.parseDouble(record.get(5));
                    double latitude = Double.parseDouble(record.get(6));
                    builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(record.get(0)));
                    count++;
                    if (count % 8192 == 0) {
                        client.writeFeatures(datastore, sft, Collections.unmodifiableList(features));
                        features.clear();
                        System.out.println("Finish count: " + count);
                    }
                }
                client.writeFeatures(datastore, sft, Collections.unmodifiableList(features));
                features.clear();
                System.out.println("Write total records: " + count);
            } catch (IOException e) {
                throw new RuntimeException("Error reading taxi data:", e);
            }

        }


    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {

            Double boundingLonMin = -74.05;
            Double boundingLonMax = -73.75;
            Double boundingLatMin = 40.60;
            Double boundingLatMax = 40.90;
            long boundingTimestampMin = IndexingSchemeDecider.fromDateToTimestamp("2010-01-01 00:00:00");
            long boundingTimestampMax = IndexingSchemeDecider.fromDateToTimestamp("2010-01-31 23:59:59");

            try {
                List<Query> queries = new ArrayList<>();

                int count = 0;
                try {
                    BufferedReader in = new BufferedReader(new FileReader("G:\\DataSet\\synthetic\\synthetic_1_30d_60_all_drz"));
                    String str;
                    while ((str = in.readLine()) != null) {
                        String[] items = str.split(",");
                        String timeMin = items[1];
                        long timestampMin = IndexingSchemeDecider.fromDateToTimestamp(timeMin);
                        String timeMax = items[2];
                        long timestampMax = IndexingSchemeDecider.fromDateToTimestamp(timeMax);
                        String lonMin = items[3];
                        String lonMax = items[4];
                        String latMin = items[5];
                        String latMax = items[6];

                        String[] dateMinItems = timeMin.split(" ");
                        String[] dateMaxItems = timeMax.split(" ");
                        String during = String.format("dtg DURING %sT%s.000Z/%sT%s.000z", dateMinItems[0], dateMinItems[1], dateMaxItems[0], dateMaxItems[1]);
                        String bbox = String.format("bbox(geom,%s, %s, %s,%s)", lonMin, lonMax, latMin, latMax);
                        System.out.println(during);
                        System.out.println(bbox);
                        Query query = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));
                        query.getHints().put(QueryHints.QUERY_INDEX(), "cymo");
                        count++;

                        queries.add(query);


                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }



                this.queries = Collections.unmodifiableList(queries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return queries;
    }

    @Override
    public Filter getSubsetFilter() {
        return Filter.INCLUDE;
    }


}
