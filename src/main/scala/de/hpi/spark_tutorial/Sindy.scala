package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, flatten}

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val dataframes = inputs.map { in =>
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(in)
    } // List of DataFrames

    val columns_map = dataframes
      .flatMap { dataframe =>
        dataframe.columns
      }
      .zipWithIndex
      .toMap

    import spark.implicits._

    val flattened_dataframes = dataframes
      .map { dataframe =>
        dataframe.flatMap { row =>
          row.schema.fields.map { field =>
            (row.getAs(field.name).toString, columns_map.get(field.name).get) // e.g. ("Germany", "COUNTRY")
          }
        }
      } // List of DataFrames of (value, column)

    val aggregated_dataframes = flattened_dataframes
      .map { dataframe =>
        dataframe
          .groupBy("_1")
          .agg(
            collect_set("_2").alias("_2")
          )
      } // List of DataFrames of (value, columns[])

    val joined_dataframes = aggregated_dataframes
      .reduce(_ union _)
      .groupBy("_1")
      .agg(
        flatten(collect_set("_2")).alias("_2")
      )
      .drop("_1") // DataFrame of (columns[])

    val inclusion_lists = joined_dataframes
      .flatMap { row =>
        val elements: List[Integer] = row.getList(0).toList
        elements.map { e =>
          (e, elements.filter(_ != e))
        }
      } // Dataset of (dependent, potential_referenced[])

    val aggregated_lists = inclusion_lists
      .groupByKey(_._1)
      .reduceGroups { (g1, g2) =>
        (g1._1, g1._2.intersect(g2._2))
      }
      .map(_._2) // Dataset of (dependent, referenced[])

    val columns_map_reverse = columns_map.map(_.swap)

    val inds = aggregated_lists
      .filter { row =>
        row._2.nonEmpty
      }
      .map { row =>
        (columns_map_reverse.get(row._1).get, row._2.map(columns_map_reverse.get(_).get).mkString(", "))
      }
      .sort("_1", "_2")

    inds.map { row =>
      row._1 + " < " + row._2
    }
      .collectAsList()
      .foreach {
        println(_)
      }
  }
}
