package com.rogerguo.data.exp2.workload3;

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
public class NYCTaxiFormattedDataExp2CymoWorkload3Equal implements CommonData {
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
        return "nyc-taxi-data-exp2-cymo-test-workload3-equal";
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
                // query 1 0.1, 1 hour
                String during1 = "dtg DURING 2010-01-12T22:00:00.000Z/2010-01-12T22:59:59.000Z";
                String bbox1 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query1 = new Query(getTypeName(), ECQL.toFilter(bbox1 + " AND " + during1));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query1.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 2 0.1, 1 hour
                String during2 = "dtg DURING 2010-01-12T18:00:00.000Z/2010-01-12T18:59:59.000Z";
                String bbox2 = "bbox(geom,-74.047000, -73.947000, 40.725000,40.825000)";
                Query query2 = new Query(getTypeName(), ECQL.toFilter(bbox2 + " AND " + during2));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query2.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 3 0.1, 1 hour
                String during3 = "dtg DURING 2010-01-22T13:00:00.000Z/2010-01-22T13:59:59.000Z";
                String bbox3 = "bbox(geom,-74.047000, -73.947000, 40.725000,40.825000)";
                Query query3 = new Query(getTypeName(), ECQL.toFilter(bbox3 + " AND " + during3));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query3.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 4 0.1, 1 hour
                String during4 = "dtg DURING 2010-01-22T12:00:00.000Z/2010-01-22T12:59:59.000Z";
                String bbox4 = "bbox(geom,-74.047000, -73.947000, 40.705000,40.805000)";
                Query query4 = new Query(getTypeName(), ECQL.toFilter(bbox4 + " AND " + during4));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query4.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 5 0.1, 1 hour
                String during5 = "dtg DURING 2010-01-29T12:00:00.000Z/2010-01-29T12:59:59.000Z";
                String bbox5 = "bbox(geom,-74.057000, -73.957000, 40.705000,40.805000)";
                Query query5 = new Query(getTypeName(), ECQL.toFilter(bbox5 + " AND " + during5));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query5.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 6 0.1, 1 hour
                String during6 = "dtg DURING 2010-01-02T08:00:00.000Z/2010-01-02T08:59:59.000Z";
                String bbox6 = "bbox(geom,-74.047000, -73.947000, 40.715000,40.815000)";
                Query query6 = new Query(getTypeName(), ECQL.toFilter(bbox6 + " AND " + during6));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query6.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // location-precedence period query set
                // query 7: 0.01, 1 month
                String during7 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-31T15:00:00.000Z";
                String bbox7 = "bbox(geom,-73.997000, -73.987000, 40.745000,40.755000)";
                Query query7 = new Query(getTypeName(), ECQL.toFilter(bbox7 + " AND " + during7));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query7.getHints().put(QueryHints.QUERY_INDEX(), "cymo");


                // query 8: 0.01, 1 month
                String during8 = "dtg DURING 2010-01-02T15:00:00.000Z/2010-01-30T15:00:00.000Z";
                String bbox8 = "bbox(geom,-73.997000, -73.987000, 40.755000,40.765000)";
                Query query8 = new Query(getTypeName(), ECQL.toFilter(bbox8 + " AND " + during8));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query8.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 9: 0.01, 1 month
                String during9 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-30T15:00:00.000Z";
                String bbox9 = "bbox(geom,-73.977000, -73.967000, 40.725000,40.735000)";
                Query query9 = new Query(getTypeName(), ECQL.toFilter(bbox9 + " AND " + during9));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query9.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // query 10 0.01, 1 month
                String during10 = "dtg DURING 2010-01-01T15:00:00.000Z/2010-01-30T15:00:00.000Z";
                String bbox10 = "bbox(geom,-73.977000, -73.967000, 40.745000,40.755000)";
                Query query10 = new Query(getTypeName(), ECQL.toFilter(bbox10 + " AND " + during10));
                //query4.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.TRUE);
                query10.getHints().put(QueryHints.QUERY_INDEX(), "cymo");

                // note: DURING is endpoint exclusive
                String during = "dtg DURING 2011-01-12T15:00:00.000Z/2011-01-12T16:59:59.000Z";
                // bounding box over most of the united states
                String bbox = "bbox(geom,-73.960000, -73.860000, 40.632000,40.732000)";
                // basic warm spatio-temporal query
                Query warmQuery = new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during));
                warmQuery.getHints().put(QueryHints.QUERY_INDEX(), "cymo");



                queries.add(warmQuery);
                queries.add(query4);
                /*queries.add(query1);
                queries.add(query2);
                queries.add(query3);
                queries.add(query4);
                queries.add(query5);
                queries.add(query6);
                queries.add(query7);
                queries.add(query8);
                queries.add(query9);
                queries.add(query10);*/

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
