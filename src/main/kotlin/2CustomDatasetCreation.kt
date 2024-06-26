package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions
import org.example.domain.Department
import org.example.domain.Employee

fun main() {
    val sparkSession = getSparkSession()

    val department1 = Department(1,"sales","john")
    val department2 = Department(2,"tech","matt")

    val employee1 = Employee(100,"james",department1)
    val employee2 = Employee(101,"prior",department1)
    val employee3 = Employee(103,"hela",department2)

    val employeeEncoder = Encoders.bean(Employee::class.java)

    //creating empDs which has department column as struct
    val empDs: Dataset<Employee> = sparkSession.createDataset(listOf(
        employee1,
        employee2,
        employee3
    ),employeeEncoder)

    empDs.printSchema()

    // get employees in sales department
    val salesEmployeeDf = empDs.filter(functions.col("department.deptName").equalTo("sales"))

    salesEmployeeDf.show(false)

//    // get all the employees in department having more than one employee
//
//    val empCountDf = empDs
//        .groupBy(functions.col("department.deptId").alias("deptId"))
//        .agg(functions.count("*").alias("employeeCount"))
//        .filter("employeeCount > 1")
//
//    val multipleEmployeesDf = empDs
//        .join(empCountDf, functions.expr("department.deptId = deptId"),"leftSemi")
//        .encode(Employee::class.java)
//
//    multipleEmployeesDf.show()


    Thread.sleep(100000000)

}