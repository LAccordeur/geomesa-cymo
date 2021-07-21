package org.locationtech.geomesa.index.index

import java.time.ZonedDateTime

import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.jts.geom.Geometry

/**
 * @Description
 * @Date 6/3/20 6:28 PM
 * @Created by rogerguo
 */
package object cymo {

  case class CymoIndexKey(partitionId: Int, subspaceId: Long, cellId: Long) extends Ordered[CymoIndexKey] {
    override def compare(that: CymoIndexKey): Int = {
      val res1 = Ordering.Int.compare(partitionId, that.partitionId)
      if (res1 != 0) { res1 } else {
        val res2 = Ordering.Long.compare(subspaceId, that.subspaceId)
        if (res2 != 0) {res2} else {
          Ordering.Long.compare(cellId, that.cellId)
        }
      }
    }
  }

  case class CymoIndexValues(geometries: FilterValues[Geometry],
                           spatialBounds: Seq[(Double, Double, Double, Double)],
                             intervals: FilterValues[Bounds[ZonedDateTime]],
                           temporalBounds: Seq[(Long, Long)],
                             sfc: Z3SFC,
                             z3TemporalBounds: Map[Short, Seq[(Long, Long)]])

}