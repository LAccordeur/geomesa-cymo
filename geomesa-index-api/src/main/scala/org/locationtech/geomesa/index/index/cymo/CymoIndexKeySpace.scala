package org.locationtech.geomesa.index.index.cymo

import java.util.Date

import com.rogerguo.cymo.entity.{SpatialRange, TimeRange}
import com.rogerguo.cymo.hbase.RowKeyHelper
import com.rogerguo.cymo.virtual.{VirtualLayer, VirtualLayerGeoMesa}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod, Z3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api.{BoundedByteRange, BoundedRange, ByteRange, IndexKeySpace, LowerBoundedRange, RowKeyValue, ScanRange, ShardStrategy, SingleRowKeyValue, UnboundedRange, UpperBoundedRange, WritableFeature}
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.z3.{Z3IndexKey, Z3IndexValues}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters
import scala.util.control.NonFatal

/**
 * @Description
 * @Date 6/3/20 6:21 PM
 * @Created by rogerguo
 */
class CymoIndexKeySpace(val sft: SimpleFeatureType,
                        val sharding: ShardStrategy,
                        geomField: String,
                        dtgField: String) extends IndexKeySpace[CymoIndexValues, CymoIndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(classOf[Point].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a point binding, but instead it has: " +
      sft.getDescriptor(geomField).getType.getBinding.getSimpleName)
  require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
    s"Expected field $dtgField to have a date binding, but instead it has: " +
      sft.getDescriptor(dtgField).getType.getBinding.getSimpleName)

  protected val testSubspaceId : Short = 1;
  protected val testEncodedValue = 2;

  // z3 filter param start
  protected val sfc = Z3SFC(TimePeriod.Day)  // note: static configuration when using Z3 filter in cymo

  protected val timeToIndex: TimeToBinnedTime = BinnedTime.timeToBinnedTime(TimePeriod.Day)

  private val dateToIndex = BinnedTime.dateToBinnedTime(TimePeriod.Day)
  private val boundsToDates = BinnedTime.boundsToIndexableDates(TimePeriod.Day)
  // z3 filter param end

  protected val geomIndex: Int = sft.indexOf(geomField)
  protected val dtgIndex: Int = sft.indexOf(dtgField)

  override val attributes: Seq[String] = Seq(geomField, dtgField)

  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(16 + sharding.length)

  override val sharing: Array[Byte] = Array.empty

  private val virtualLayer: VirtualLayerGeoMesa = new VirtualLayerGeoMesa("127.0.0.1");

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[CymoIndexKey] = {
    val geom = writable.getAttribute[Point](geomIndex)
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
    }
    val dtg = writable.getAttribute[Date](dtgIndex)
    val time = if (dtg == null) { 0 } else { dtg.getTime }

    import com.rogerguo.cymo.hbase._

    // TODO check the time format
    val cymoRowKeyItem : RowKeyItem = RowKeyHelper.generateDataTableRowKeyForGeoMesa(geom.getX, geom.getY, time)
    val bytesRowKey = cymoRowKeyItem.getBytesRowKey

    val shard = sharding(writable)

    // create the byte array - allocate a single array up front to contain everything
    // ignore tier, not used here
    /*val bytes = Array.ofDim[Byte](shard.length + 16 + id.length)

    if (shard.isEmpty) {
      System.arraycopy(bytesRowKey, 0, bytes, 0, bytesRowKey.length)
      System.arraycopy(id, 0, bytes, 16, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      System.arraycopy(bytesRowKey, 0, bytes, 1, bytesRowKey.length)
      System.arraycopy(id, 0, bytes, 17, id.length)
    }*/


    // z3 filter parameter start
    // add z3 between cell index key and feature id
    val BinnedTime(b, t) = timeToIndex(time)
    val z = try { sfc.index(geom.getX, geom.getY, t, lenient).z } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
    }

    val bytes = Array.ofDim[Byte](shard.length + 26 + id.length)

    if (shard.isEmpty) {
      System.arraycopy(bytesRowKey, 0, bytes, 0, bytesRowKey.length)

      ByteArrays.writeShort(b, bytes, 16)
      ByteArrays.writeLong(z, bytes, 18)

      System.arraycopy(id, 0, bytes, 26, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      System.arraycopy(bytesRowKey, 0, bytes, 1, bytesRowKey.length)

      ByteArrays.writeShort(b, bytes, 17)
      ByteArrays.writeLong(z, bytes, 19)

      System.arraycopy(id, 0, bytes, 27, id.length)
    }

    // z3 filter parameter end

    SingleRowKeyValue(bytes, sharing, shard, CymoIndexKey(cymoRowKeyItem.getPartitionID, cymoRowKeyItem.getSubspaceID, cymoRowKeyItem.getCellID), tier, id, writable.values)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): CymoIndexValues = {

    import org.locationtech.geomesa.filter.FilterHelper._

    // standardize the two key query arguments:  polygon and date-range

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, geomField, intersect = true) // intersect since we have points
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = extractIntervals(filter, dtgField, handleExclusiveBounds = true)


    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    // TODO check null and out-of-range date value
    val temporalBounds = Seq.newBuilder[(Long, Long)]
    intervals.foreach { interval =>
      val (lower, upper) = interval.bounds
      val lowerTimestamp = lower.get.toInstant.toEpochMilli
      val upperTimestamp = upper.get.toInstant.toEpochMilli
      temporalBounds += ((lowerTimestamp, upperTimestamp))
    }


    // z3 filter start

    val minTime = sfc.time.min.toLong
    val maxTime = sfc.time.max.toLong
    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
    val unboundedBins = Seq.newBuilder[(Short, Short)]

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)

      if (interval.isBoundedBothSides) {
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt, ut))
        } else {
          timesByBin(lb) ++= Seq((lt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = sfc.wholePeriod)
        }
      } else if (interval.lower.value.isDefined) {
        timesByBin(lb) ++= Seq((lt, maxTime))
        unboundedBins += (((lb + 1).toShort, Short.MaxValue))
      } else if (interval.upper.value.isDefined) {
        timesByBin(ub) ++= Seq((minTime, ut))
        unboundedBins += ((0, (ub - 1).toShort))
      }
    }

    // z3 filter end

    // xy: spatial region; temporalBounds: timestamp (both not normalized yet)
    CymoIndexValues(geometries, xy, intervals, temporalBounds.result(), sfc, timesByBin.toMap)
  }

  override def getRanges(values: CymoIndexValues, multiplier: Int): Iterator[ScanRange[CymoIndexKey]] = {
    val CymoIndexValues( _, xy, _, temporalBounds, _, _) = values

    val spatialRangeArray = xy.toArray
    val temporalRangeArray = temporalBounds.toArray

    val longitudeRange : SpatialRange = new SpatialRange(spatialRangeArray(0)._1, spatialRangeArray(0)._3)
    val latitudeRange : SpatialRange = new SpatialRange(spatialRangeArray(0)._2, spatialRangeArray(0)._4)
    val timeRange : TimeRange = new TimeRange(temporalRangeArray(0)._1, temporalRangeArray(0)._2);

    val scanRangesFromVirtualLayer = virtualLayer.getRanges(longitudeRange, latitudeRange, timeRange)

    import scala.collection.JavaConverters._
    var scanRangeList = scanRangesFromVirtualLayer.asScala.toList.map(x => BoundedRange(CymoIndexKey(x.getPartitionID, x.getSubspaceID, x.getCellIDLowBound), CymoIndexKey(x.getPartitionID, x.getSubspaceID, x.getCellIDHighBound)))

    scanRangeList.toIterator
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[CymoIndexKey]], tier: Boolean): Iterator[ByteRange] = {
    if (sharding.length == 0) {
      ranges.map {
        case BoundedRange(lo, hi) =>
          BoundedByteRange(RowKeyHelper.concatDataTableByteRowKey(lo.partitionId, lo.subspaceId, lo.cellId), RowKeyHelper.concatDataTableByteRowKey(hi.partitionId, hi.subspaceId, hi.cellId))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = RowKeyHelper.concatDataTableByteRowKey(lo.partitionId, lo.subspaceId, lo.cellId)
          val upper = RowKeyHelper.concatDataTableByteRowKey(hi.partitionId, hi.subspaceId, hi.cellId)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[CymoIndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if the user has requested strict bounding boxes, we apply the full filter
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    // if we have unbounded dates, we need to pass them through as we don't have z-values for all periods

    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    //val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(config.forall(_.queries.looseBBox))
    def complexGeoms: Boolean = values.exists(_.geometries.values.exists(g => !GeometryUtils.isRectangular(g)))
    complexGeoms
  }
}

object CymoIndexKeySpace extends IndexKeySpaceFactory[CymoIndexValues, CymoIndexKey] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(2) == 0 && attributes.forall(sft.indexOf(_) != -1) &&
      classOf[Point].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding) &&
      classOf[Date].isAssignableFrom(sft.getDescriptor(attributes.last).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): CymoIndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new CymoIndexKeySpace(sft, shards, attributes.head, attributes.last)
  }
}