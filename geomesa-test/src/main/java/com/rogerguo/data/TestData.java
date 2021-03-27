package com.rogerguo.data;

import com.rogerguo.client.old.GeoMesaClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.factory.Hints;
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
 * @Date 2019/7/11 10:09
 * @Created by X1 Carbon
 */
public class TestData  implements CommonData {
    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;

    @Override
    public String getTypeName() {
        return "test-data";
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            // list the attributes that constitute the feature type
            // this is a reduced set of the attributes from GDELT 2.0
            StringBuilder attributes = new StringBuilder();
            attributes.append("objectId:String,");
            attributes.append("dtg:Date,");
            attributes.append("*geom:Point:srid=4326"); // the "*" denotes the default geometry (used for indexing)

            // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
            // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());

            // use the user-data (hints) to specify which date field to use for primary indexing
            // if not specified, the first date attribute (if any) will be used
            // could also use ':default=true' in the attribute specification string
            sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        }
        return sft;
    }

    @Override
    public void writeTestData(GeoMesaClient client, DataStore datastore, SimpleFeatureType sft) {

    }

    @Override
    public List<SimpleFeature> getTestData() {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the bundled t-drive CSV
            URL input = getClass().getClassLoader().getResource("dataset/test.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource test.csv");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

            // use a geotools SimpleFeatureBuilder to create our features
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
                for (CSVRecord record : parser) {
                    // pull out the fields corresponding to our simple feature attributes
                    builder.set("objectId", record.get(0));

                    // some dates are converted implicitly, so we can set them as strings
                    // however, the date format here isn't one that is converted, so we parse it into a java.util.Date
                    //System.out.println(record.get(0));
                    builder.set("dtg", Date.from(LocalDateTime.parse(record.get(1), dateFormat).toInstant(ZoneOffset.UTC)));

                    // we can use WKT (well-known-text) to represent geometries
                    // note that we use longitude first ordering
                    double longitude = Double.parseDouble(record.get(2));
                    double latitude = Double.parseDouble(record.get(3));
                    builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                    // be sure to tell GeoTools explicitly that we want to use the ID we provided
                    builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // build the feature - this also resets the feature builder for the next entry
                    // use the taxi ID as the feature ID
                    features.add(builder.buildFeature(record.get(0)));
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading test data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
        return features;
    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {

            try {
                List<Query> queries = new ArrayList<>();


                // note: DURING is endpoint exclusive
                String during = "dtg DURING 2014-04-25T15:00:00.000Z/2014-04-25T15:35:00.000Z";
                // bounding box over most of the united states
                String bbox = "bbox(geom,-80,-60,30,50)";

                // basic spatio-temporal query
                //queries.add(new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during)));

                queries.add(new Query(getTypeName(), Filter.INCLUDE));

                String queryPoint = "featureId = B02682";
                //queries.add(new Query(getTypeName(), ECQL.toFilter(queryPoint)));
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
