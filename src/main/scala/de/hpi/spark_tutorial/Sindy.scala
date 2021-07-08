package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, flatten}

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val dataframes = inputs.map { in =>
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(in)
    } // List of DataFrames

    import spark.implicits._

    //val columnToID = dataframes.flatMap(dataframe => dataframe.columns).zipWithIndex.toMap
    val flattened_dataframes = dataframes
      .map { dataframe =>
        dataframe.flatMap { row =>
          row.schema.fields.map { field =>
            (row.getAs(field.name).toString, field.name) // e.g. ("Germany", "COUNTRY")
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
      .drop("_1") // List of DataFrames of (columns[])

    val inclusion_lists = joined_dataframes
      .flatMap { row =>
        val elements: List[String] = row.getList(0).toList
        elements.map { e =>
          (e, elements.filter(_ != e))
        }
      }

    val aggregated_lists = inclusion_lists
      .groupBy("_1")
      .agg(
        collect_set("_2").alias("_2")
      )

    val intersected = aggregated_lists
      .map { row =>
        val elements: List[mutable.WrappedArray[String]] = row.getList(1).toList
        val intersection = elements.head.toList
        elements.drop(1).foreach {
          intersection.intersect(_)
        }
        (row.getString(0), intersection)
      }

    joined_dataframes.show(50, false)

    val inds = intersected
      .filter { row =>
        row._2.nonEmpty
      }
      .sort("_1")

    inds.map { row =>
      row._1 + " < " + row._2.mkString(", ")
    }
      .collectAsList()
      .foreach {
        println(_)
      }
  }
}
