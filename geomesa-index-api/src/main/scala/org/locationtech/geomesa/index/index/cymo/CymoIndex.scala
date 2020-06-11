package org.locationtech.geomesa.index.index.cymo

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

/**
 * @Description
 * @Date 6/3/20 6:20 PM
 * @Created by rogerguo
 */
class CymoIndex protected (
                            ds: GeoMesaDataStore[_],
                            sft: SimpleFeatureType,
                            version: Int,
                            val geom: String,
                            val dtg: String,
                            mode: IndexMode
                          ) extends GeoMesaFeatureIndex[CymoIndexValues, CymoIndexKey](ds, sft, CymoIndex.name, version, Seq(geom, dtg), mode)
  with SpatioTemporalFilterStrategy[CymoIndexValues, CymoIndexKey] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geomField: String, dtgField: String, mode: IndexMode) =
    this(ds, sft, CymoIndex.version, geomField, dtgField, mode)

  override val keySpace: CymoIndexKeySpace = new CymoIndexKeySpace(sft, ZShardStrategy(sft), geom, dtg)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object CymoIndex extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "cymo"
  override val version = 1

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    CymoIndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = {
    if (sft.isPoints && sft.getDtgField.isDefined) { Seq(Seq(sft.getGeomField, sft.getDtgField.get)) } else { Seq.empty }
  }
}
