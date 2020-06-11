package org.locationtech.geomesa.hbase.cymo

/**
 * @Description
 * @Date 6/4/20 3:04 PM
 * @Created by rogerguo
 */
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, HBaseCatalogParam}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseCymoIndexTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  "CymoIndex" should {
    "work with HBase" in {
      val typeName = "testCymo"

      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName
      )
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          "name:String,track:String,dtg:Date,*geom:Point:srid=4326;geomesa.indices.enabled=cymo:geom:dtg"))
        val sft = ds.getSchema(typeName)

        val features =
          (0 until 10).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track1", s"2010-05-07T0$i:00:00.000Z", s"POINT(4$i 60)")
          } ++ (10 until 20).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track2", s"2010-05-${i}T$i:00:00.000Z", s"POINT(4${i - 10} 60)")
          } ++ (20 until 30).map { i =>
            ScalaSimpleFeature.create(sft, s"$i", s"name$i", "track3", s"2010-05-${i}T${i-10}:00:00.000Z", s"POINT(6${i - 20} 60)")
          }

        WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach(f => FeatureUtils.copyToWriter(writer, f, useProvidedFid = true))
        }

        def runQuery(query: Query): Seq[SimpleFeature] =
          SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList

        { // return all features for inclusive filter
          val filter = "bbox(geom, 38, 59, 51, 61)" +
            " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(10)
          //features.map(_.getID.toInt) must containTheSameElementsAs(0 to 9)
        }

        /*{ // return some features for exclusive geom filter
          val filter = "bbox(geom, 38, 59, 45, 61)" +
            " AND dtg between '2010-05-07T00:00:00.000Z' and '2010-05-08T00:00:00.000Z'"
          val features = runQuery(new Query(sft.getTypeName, ECQL.toFilter(filter)))
          features must haveSize(6)
          features.map(_.getID.toInt) must containTheSameElementsAs(0 to 5)
        }*/

      } finally {
        ds.dispose()
      }
    }
  }
}