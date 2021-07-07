package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

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
            collect_set("_2")
          )
      } // List of DataFrames of (value, columns[])

    val joined_dataframes = aggregated_dataframes
      .reduce(_ union _)
      .groupBy("_1")
      .agg(
        collect_set("collect_set(_2)")
      )

    val attribute_groups = joined_dataframes
      .map { row =>
        row.get(1).toString
      }

    attribute_groups.show()
    /*aggregated_dataframes.foreach { it =>
      it.show()
    }*/


  }
}
