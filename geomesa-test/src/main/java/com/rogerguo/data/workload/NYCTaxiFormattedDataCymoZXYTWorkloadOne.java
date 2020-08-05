package com.rogerguo.data.workload;

import com.rogerguo.client.GeoMesaClient;
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
public class NYCTaxiFormattedDataCymoZXYTWorkloadOne implements CommonData {
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
        return "nyc-taxi-data-cymo-test-workload-zxyt";
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


                //time-precedence query workload
                // query 11 0.1, 1 hour
                String during11 = "dtg DURING 2010-01-16T22:00:00.000Z/2010-01-16T22:59:59.000Z";
                String bbox11 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query11 = new Query(getTypeName(), ECQL.toFilter(bbox11 + " AND " + during11));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query11.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 12 0.1, 1 hour
                String during12 = "dtg DURING 2010-01-02T18:00:00.000Z/2010-01-02T18:59:59.000Z";
                String bbox12 = "bbox(geom,-74.047000, -73.947000, 40.725000,40.825000)";
                Query query12 = new Query(getTypeName(), ECQL.toFilter(bbox12 + " AND " + during12));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query12.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 13 0.1, 1 hour
                String during13 = "dtg DURING 2010-01-02T12:00:00.000Z/2010-01-02T12:59:59.000Z";
                String bbox13 = "bbox(geom,-74.047000, -73.947000, 40.725000,40.825000)";
                Query query13 = new Query(getTypeName(), ECQL.toFilter(bbox13 + " AND " + during13));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query13.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 14 0.1, 1 hour
                String during14 = "dtg DURING 2010-01-16T12:00:00.000Z/2010-01-16T12:59:59.000Z";
                String bbox14 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query14 = new Query(getTypeName(), ECQL.toFilter(bbox14 + " AND " + during14));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query14.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 15 0.1, 1 hour
                String during15 = "dtg DURING 2010-01-29T12:00:00.000Z/2010-01-29T12:59:59.000Z";
                String bbox15 = "bbox(geom,-74.057000, -73.957000, 40.705000,40.805000)";
                Query query15 = new Query(getTypeName(), ECQL.toFilter(bbox15 + " AND " + during15));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query15.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 16 0.1, 1 hour
                String during16 = "dtg DURING 2010-01-02T08:00:00.000Z/2010-01-02T08:59:59.000Z";
                String bbox16 = "bbox(geom,-74.047000, -73.947000, 40.715000,40.815000)";
                Query query16 = new Query(getTypeName(), ECQL.toFilter(bbox16 + " AND " + during16));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query16.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 17 0.1, 1 hour
                String during17 = "dtg DURING 2010-01-02T12:00:00.000Z/2010-01-02T12:59:59.000Z";
                String bbox17 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query17 = new Query(getTypeName(), ECQL.toFilter(bbox17 + " AND " + during17));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query17.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 18 0.1, 1 hour
                String during18 = "dtg DURING 2010-01-02T01:00:00.000Z/2010-01-02T01:59:59.000Z";
                String bbox18 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query18 = new Query(getTypeName(), ECQL.toFilter(bbox18 + " AND " + during18));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query18.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 19 0.1, 1 hour
                String during19 = "dtg DURING 2010-01-16T16:00:00.000Z/2010-01-16T16:59:59.000Z";
                String bbox19 = "bbox(geom,-74.057000, -73.957000, 40.705000,40.805000)";
                Query query19 = new Query(getTypeName(), ECQL.toFilter(bbox19 + " AND " + during19));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query19.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 20 0.1, 1 hour
                String during20 = "dtg DURING 2010-01-29T04:00:00.000Z/2010-01-29T04:59:59.000Z";
                String bbox20 = "bbox(geom,-74.057000, -73.957000, 40.705000,40.805000)";
                Query query20 = new Query(getTypeName(), ECQL.toFilter(bbox20 + " AND " + during20));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query20.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // note: DURING is endpoint exclusive
                String during = "dtg DURING 2011-01-12T15:00:00.000Z/2011-01-12T16:59:59.000Z";
                // bounding box over most of the united states
                String bbox = "bbox(geom,-73.960000, -73.860000, 40.632000,40.732000)";
                // basic warm spatio-temporal query
                Query warmQuery = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));
                warmQuery.getHints().put(QueryHints.QUERY_INDEX(), "cymo");



                queries.add(warmQuery);
                queries.add(query20);
                /*queries.add(query11);
                queries.add(query12);
                queries.add(query13);
                queries.add(query14);
                queries.add(query15);
                queries.add(query16);
                queries.add(query17);
                queries.add(query18);
                queries.add(query19);
                queries.add(query20);*/

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
