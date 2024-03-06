package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row


fun <T> Dataset<Row>.encode(clazz: Class<T>):Dataset<T>{
    return this.`as`(Encoders.bean(clazz))
}