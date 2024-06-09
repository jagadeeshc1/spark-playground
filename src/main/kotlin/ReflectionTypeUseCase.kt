package org.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*


@NoArgEntity
data class Student(
    var studentId:Int,
    var sectionId:Int,
    var name:String,
    var mathScore:Int,
    var scienceScore:Int,
    var englishScore:Int,
)

@NoArgEntity
data class Section(
    var sectionId:Int,
    var mandatoryScores:List<String>
)

fun getStudentDs():Dataset<Student>{
    val sparkSession = getSparkSession()

    val student1 = Student(1,100,"student1",50,60,70)
    val student2 = Student(2,200,"student2",30,20,80)
    val student3 = Student(1,100,"student3",10,60,40)

    val students = listOf(
        student1,
        student2,
        student3
    )

    return sparkSession.createDataset(students,Encoders.bean(Student::class.java))
}

fun getSectionDs():Dataset<Section>{
    val sparkSession = getSparkSession()
    val section1 = Section(100, listOf("mathScore","englishScore"))
    val section2 = Section(200, listOf("englishScore","scienceScore"))

    val sections = listOf(
        section1,
        section2
    )

    return sparkSession.createDataset(sections,Encoders.bean(Section::class.java))

}

fun simpleScoreSum(studentDs:Dataset<Student>){
    val resultDf = studentDs
        .withColumn("totalScore",
            col("englishScore").plus(col("scienceScore")).plus(col("mathScore")))

    /**
     * or
     * studentDs.withColumn("totalScore",expr("englishScore + scienceScore + mathScore"))
     *
     * both are same
     */

    resultDf.show(false)

}


fun getJoinedDs():Dataset<Row>{
    val studentDs = getStudentDs()
    val sectionDs = getSectionDs()
    val joinedDs = studentDs.join(sectionDs,"sectionId")
    joinedDs.show(false)
    return joinedDs

}

fun mandatoryScoreSumUsingGeneratedCaseWhen(joinedDs:Dataset<Row>){
    val cols = joinedDs.columns().toSet()
    var caseExpr = "CASE \n"
    for(colName in cols){
        caseExpr += " WHEN x= '$colName' then cast(cast($colName as string) as int) \n"
    }
    caseExpr += " ELSE null \n"
    caseExpr += "END \n"

    println(caseExpr)

    val result = joinedDs.withColumn(
        "mandatoryScoreValues",
        expr("""
            transform(
                mandatoryScores,
                x -> $caseExpr
            )
        """.trimIndent())
    ).withColumn(
        "mandatoryScoreSum",
        expr("""
            aggregate(mandatoryScoreValues,0,(acc,x)->acc+x)
        """.trimIndent())
    )

    result.show(false)
}


fun main(){

    val studentDs = getStudentDs()
    val sectionDs = getSectionDs()

//    simpleScoreSum(studentDs)

    val joinedDs = getJoinedDs()

    mandatoryScoreSumUsingGeneratedCaseWhen(joinedDs)

}