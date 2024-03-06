package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

fun main() {
    val sparkSession = getSparkSession()

    // create Basic Integer Dataset

    val intDs: Dataset<Int> = sparkSession.createDataset(listOf(1,2,3,5),Encoders.INT())

    val multipliedDs: Dataset<Row> = intDs
        .withColumn("multipliedValue",col("value").multiply(10))

    // print the schema
    intDs.printSchema()

    multipliedDs.printSchema()

    // see the plan
    intDs.explain()

    multipliedDs.explain()


    // show the table

    intDs.show()

    multipliedDs.show()

    Thread.sleep(100000)

}
