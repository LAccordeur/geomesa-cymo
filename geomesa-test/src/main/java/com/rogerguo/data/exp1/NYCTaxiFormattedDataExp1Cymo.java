package com.rogerguo.data.exp1;

import com.rogerguo.client.old.GeoMesaClient;
import com.rogerguo.data.CommonData;
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
public class NYCTaxiFormattedDataExp1Cymo implements CommonData {
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
        return "nyc-taxi-data-exp1-cymo-test";
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

            try {
                List<Query> queries = new ArrayList<>();


                // note: DURING is endpoint exclusive
                String during = "dtg DURING 2011-01-12T15:00:00.000Z/2011-01-12T16:59:59.000Z";
                // bounding box over most of the united states
                String bbox = "bbox(geom,-73.960000, -73.860000, 40.632000,40.732000)";
                // basic warm spatio-temporal query
                Query warmQuery = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));
                warmQuery.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 1 - 0.001, 1 hour - test 1  total points 0
                String during1 = "dtg DURING 2010-01-02T15:00:00.000Z/2010-01-02T15:59:59.000Z";
                // bounding box over most of the united states
                String bbox1 = "bbox(geom,-73.802000, -73.801000, 40.675000,40.676000)";
                Query query1 = new Query(getTypeName(), ECQL.toFilter(bbox1 + " AND " + during1));
                //query1.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query1.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 1 - 0.001, 1 hour - test 2  total points 0
                String during2 = "dtg DURING 2010-01-11T01:00:00.000Z/2010-01-11T01:59:59.000Z";
                // bounding box over most of the united states
                String bbox2 = "bbox(geom,-73.885000, -73.884000, 40.746000,40.747000)";
                Query query2 = new Query(getTypeName(), ECQL.toFilter(bbox2 + " AND " + during2));
                //query2.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query2.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 1 - 0.001, 1 hour - test 3   total points 62
                String during3 = "dtg DURING 2010-01-01T01:00:00.000Z/2010-01-01T01:59:59.000Z";
                String bbox3 = "bbox(geom,-73.992000, -73.991000, 40.749000,40.750000)";
                Query query3 = new Query(getTypeName(), ECQL.toFilter(bbox3 + " AND " + during3));
                //query3.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query3.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 2 - 0.001, 1 day - test 1  total size 1
                String during4 = "dtg DURING 2010-01-02T15:00:00.000Z/2010-01-03T15:00:00.000Z";
                String bbox4 = "bbox(geom,-73.802000, -73.801000, 40.675000,40.676000)";
                Query query4 = new Query(getTypeName(), ECQL.toFilter(bbox4 + " AND " + during4));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query4.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 2 - 0.001, 1 day - test 2  total size 1
                String during5 = "dtg DURING 2010-01-18T15:00:00.000Z/2010-01-19T15:00:00.000Z";
                String bbox5 = "bbox(geom,-73.885000, -73.884000, 40.746000,40.747000)";
                Query query5 = new Query(getTypeName(), ECQL.toFilter(bbox5 + " AND " + during5));
                //query2.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query5.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 2 - 0.001, 1 day - test 3  total points 1587
                String during6 = "dtg DURING 2010-01-26T19:00:00.000Z/2010-01-27T19:00:00.000Z";
                String bbox6 = "bbox(geom,-73.992000, -73.991000, 40.749000,40.750000)";
                Query query6 = new Query(getTypeName(), ECQL.toFilter(bbox6 + " AND " + during6));
                //query3.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query6.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 3 - 0.001, 1 week - test 1  total points 8
                String during7 = "dtg DURING 2010-01-02T15:00:00.000Z/2010-01-09T15:00:00.000Z";
                String bbox7 = "bbox(geom,-73.802000, -73.801000, 40.675000,40.676000)";
                Query query7 = new Query(getTypeName(), ECQL.toFilter(bbox7 + " AND " + during7));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query7.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 3 - 0.001, 1 week - test 2  total points 2
                String during8 = "dtg DURING 2010-01-18T15:00:00.000Z/2010-01-25T15:00:00.000Z";
                String bbox8 = "bbox(geom,-73.885000, -73.884000, 40.746000,40.747000)";
                Query query8 = new Query(getTypeName(), ECQL.toFilter(bbox8 + " AND " + during8));
                //query2.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query8.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 3 - 0.001, 1 week - test 3  total points 11342
                String during9 = "dtg DURING 2010-01-10T19:00:00.000Z/2010-01-17T19:00:00.000Z";
                String bbox9 = "bbox(geom,-73.992000, -73.991000, 40.749000,40.750000)";
                Query query9 = new Query(getTypeName(), ECQL.toFilter(bbox9 + " AND " + during9));
                //query3.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query9.getHints().put(QueryHints.QUERY_INDEX(), "cymo");



                // query 4 - 0.001, 1 month - test 1  total points 27
                String during10 = "dtg DURING 2010-01-02T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox10 = "bbox(geom,-73.802000, -73.801000, 40.675000,40.676000)";
                Query query10 = new Query(getTypeName(), ECQL.toFilter(bbox10 + " AND " + during10));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query10.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 4 - 0.001, 1 month - test 2 total points 14
                String during11 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox11 = "bbox(geom,-73.885000, -73.884000, 40.746000,40.747000)";
                Query query11 = new Query(getTypeName(), ECQL.toFilter(bbox11 + " AND " + during11));
                //query2.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query11.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 4 - 0.001, 1 month - test 3  total points 49536
                String during12 = "dtg DURING 2010-01-01T19:00:00.000Z/2010-01-31T19:00:00.000Z";
                String bbox12 = "bbox(geom,-73.992000, -73.991000, 40.749000,40.750000)";
                Query query12 = new Query(getTypeName(), ECQL.toFilter(bbox12 + " AND " + during12));
                //query3.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query12.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 5 - 0.01, 1 hour - test 1  total points 0
                String during13 = "dtg DURING 2010-01-04T15:00:00.000Z/2010-01-04T15:59:59.000Z";
                String bbox13 = "bbox(geom,-73.807000, -73.797000, 40.670000,40.680000)";
                Query query13 = new Query(getTypeName(), ECQL.toFilter(bbox13 + " AND " + during13));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query13.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 5 - 0.01, 1 hour - test 2 total points 2
                String during14 = "dtg DURING 2010-01-14T15:00:00.000Z/2010-01-14T15:59:59.000Z";
                String bbox14 = "bbox(geom,-73.890000, -73.880000, 40.740000,40.750000)";
                Query query14 = new Query(getTypeName(), ECQL.toFilter(bbox14 + " AND " + during14));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query14.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 5 - 0.01, 1 hour - test 3 total points 1383
                String during15 = "dtg DURING 2010-01-04T19:00:00.000Z/2010-01-04T19:59:59.000Z";
                String bbox15 = "bbox(geom,-73.997000, -73.987000, 40.745000,40.755000)";
                Query query15 = new Query(getTypeName(), ECQL.toFilter(bbox15 + " AND " + during15));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query15.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 6 - 0.01, 1 day - test 1 total points 51
                String during16 = "dtg DURING 2010-01-03T11:00:00.000Z/2010-01-04T11:00:00.000Z";
                String bbox16 = "bbox(geom,-73.807000, -73.797000, 40.670000,40.680000)";
                Query query16 = new Query(getTypeName(), ECQL.toFilter(bbox16 + " AND " + during16));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query16.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 6 - 0.01, 1 day - test 2  total points 73
                String during17 = "dtg DURING 2010-01-14T15:00:00.000Z/2010-01-15T15:00:00.000Z";
                String bbox17 = "bbox(geom,-73.890000, -73.880000, 40.740000,40.750000)";
                Query query17 = new Query(getTypeName(), ECQL.toFilter(bbox17 + " AND " + during17));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query17.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 6 - 0.01, 1 day - test 3  total points 23358
                String during18 = "dtg DURING 2010-01-04T15:00:00.000Z/2010-01-05T15:00:00.000Z";
                String bbox18 = "bbox(geom,-73.997000, -73.987000, 40.745000,40.755000)";
                Query query18 = new Query(getTypeName(), ECQL.toFilter(bbox18 + " AND " + during18));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query18.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 7 - 0.01, 1 week - test 1  total points 254
                String during19 = "dtg DURING 2010-01-03T11:00:00.000Z/2010-01-10T11:00:00.000Z";
                String bbox19 = "bbox(geom,-73.807000, -73.797000, 40.670000,40.680000)";
                Query query19 = new Query(getTypeName(), ECQL.toFilter(bbox19 + " AND " + during19));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query19.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 7 - 0.01, 1 week - test 2 total points 482
                String during20 = "dtg DURING 2010-01-14T15:00:00.000Z/2010-01-21T15:00:00.000Z";
                String bbox20 = "bbox(geom,-73.890000, -73.880000, 40.740000,40.750000)";
                Query query20 = new Query(getTypeName(), ECQL.toFilter(bbox20 + " AND " + during20));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query20.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 7 - 0.01, 1 week - test 3  total points 172724
                String during21 = "dtg DURING 2010-01-04T15:00:00.000Z/2010-01-11T15:00:00.000Z";
                String bbox21 = "bbox(geom,-73.997000, -73.987000, 40.745000,40.755000)";
                Query query21 = new Query(getTypeName(), ECQL.toFilter(bbox21 + " AND " + during21));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query21.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 8 - 0.01, 1 month - test 1  total points 1098
                String during22 = "dtg DURING 2010-01-03T11:00:00.000Z/2010-01-31T11:00:00.000Z";
                String bbox22 = "bbox(geom,-73.807000, -73.797000, 40.670000,40.680000)";
                Query query22 = new Query(getTypeName(), ECQL.toFilter(bbox22 + " AND " + during22));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query22.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 8 - 0.01, 1 month - test 2  total points 1962
                String during23 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox23 = "bbox(geom,-73.890000, -73.880000, 40.740000,40.750000)";
                Query query23 = new Query(getTypeName(), ECQL.toFilter(bbox23 + " AND " + during23));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query23.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 8 - 0.01, 1 month - test 3 total points 741246
                String during24 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox24 = "bbox(geom,-73.997000, -73.987000, 40.745000,40.755000)";
                Query query24 = new Query(getTypeName(), ECQL.toFilter(bbox24 + " AND " + during24));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query24.getHints().put(QueryHints.QUERY_INDEX(), "cymo");



                // query 9 - 0.1, 1 hour - test 1  total points 395
                String during25 = "dtg DURING 2010-01-08T15:00:00.000Z/2010-01-08T15:59:59.000Z";
                String bbox25 = "bbox(geom,-73.857000, -73.757000, 40.620000,40.720000)";
                Query query25 = new Query(getTypeName(), ECQL.toFilter(bbox25 + " AND " + during25));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query25.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 9 - 0.1, 1 hour - test 2 total points  632
                String during26 = "dtg DURING 2010-01-02T19:00:00.000Z/2010-01-02T19:59:59.000Z";
                String bbox26 = "bbox(geom,-73.940000, -73.840000, 40.700000,40.800000)";
                Query query26 = new Query(getTypeName(), ECQL.toFilter(bbox26 + " AND " + during26));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query26.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 9 - 0.1, 1 hour - test 3  total points 23755
                String during27 = "dtg DURING 2010-01-12T22:00:00.000Z/2010-01-12T22:59:59.000Z";
                String bbox27 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query27 = new Query(getTypeName(), ECQL.toFilter(bbox27 + " AND " + during27));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query27.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 10 - 0.1, 1 day - test 1  total points 5227
                String during28 = "dtg DURING 2010-01-08T15:00:00.000Z/2010-01-09T15:00:00.000Z";
                String bbox28 = "bbox(geom,-73.857000, -73.757000, 40.620000,40.720000)";
                Query query28 = new Query(getTypeName(), ECQL.toFilter(bbox28 + " AND " + during28));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query28.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 10 - 0.1, 1 day - test 2  total points 11971
                String during29 = "dtg DURING 2010-01-02T05:00:00.000Z/2010-01-03T05:00:00.000Z";
                String bbox29 = "bbox(geom,-73.940000, -73.840000, 40.700000,40.800000)";
                Query query29 = new Query(getTypeName(), ECQL.toFilter(bbox29 + " AND " + during29));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query29.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 10 - 0.1, 1 day - test 3  total points 454428
                String during30 = "dtg DURING 2010-01-12T22:00:00.000Z/2010-01-13T22:00:00.000Z";
                String bbox30 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query30 = new Query(getTypeName(), ECQL.toFilter(bbox30 + " AND " + during30));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query30.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 11 - 0.1, 1 week - test 1  total points 40798
                String during31 = "dtg DURING 2010-01-08T15:00:00.000Z/2010-01-15T15:00:00.000Z";
                String bbox31 = "bbox(geom,-73.857000, -73.757000, 40.620000,40.720000)";
                Query query31 = new Query(getTypeName(), ECQL.toFilter(bbox31 + " AND " + during31));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query31.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 11 - 0.1, 1 week - test 2  total points 93877
                String during32 = "dtg DURING 2010-01-02T05:00:00.000Z/2010-01-09T05:00:00.000Z";
                String bbox32 = "bbox(geom,-73.940000, -73.840000, 40.700000,40.800000)";
                Query query32 = new Query(getTypeName(), ECQL.toFilter(bbox32 + " AND " + during32));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query32.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 11 - 0.1, 1 week - test 3  total points 2983575
                String during33 = "dtg DURING 2010-01-12T22:00:00.000Z/2010-01-19T22:00:00.000Z";
                String bbox33 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query33 = new Query(getTypeName(), ECQL.toFilter(bbox33 + " AND " + during33));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query33.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 12 - 0.1, 1 month - test 1  total points 177150
                String during34 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox34 = "bbox(geom,-73.857000, -73.757000, 40.620000,40.720000)";
                Query query34 = new Query(getTypeName(), ECQL.toFilter(bbox34 + " AND " + during34));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query34.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 12 - 0.1, 1 month - test 2  total points 388602
                String during35 = "dtg DURING 2010-01-02T05:00:00.000Z/2010-01-31T05:00:00.000Z";
                String bbox35 = "bbox(geom,-73.940000, -73.840000, 40.700000,40.800000)";
                Query query35 = new Query(getTypeName(), ECQL.toFilter(bbox35 + " AND " + during35));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query35.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 12 - 0.1, 1 month - test 3  total points 13025325
                String during36 = "dtg DURING 2010-01-01T22:00:00.000Z/2010-01-31T22:00:00.000Z";
                String bbox36 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query36 = new Query(getTypeName(), ECQL.toFilter(bbox36 + " AND " + during36));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query36.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                queries.add(warmQuery);
                //queries.add(query34);
                //queries.add(query35);
                queries.add(query27);

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
