package org.example

import org.apache.spark.sql.SparkSession

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {

    val sparkSession = getSparkSession()


}

fun getSparkSession():SparkSession{
    return SparkSession.builder().master("local[*]").orCreate
}