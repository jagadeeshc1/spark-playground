package org.example

import org.apache.spark.sql.Encoders
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

fun main(){

    val sparkSession = getSparkSession()

    val student1 = Student(1,100,"student1",50,60,70)
    val student2 = Student(2,200,"student2",30,20,80)
    val student3 = Student(1,100,"student3",10,60,40)

    val students = listOf(
        student1,
        student2,
        student2
    )

    val studentDs = sparkSession.createDataset(students,Encoders.bean(Student::class.java))

    val section1 = Section(100, listOf("mathScore","englishScore"))
    val section2 = Section(200, listOf("englishScore","scienceScore"))

    val sections = listOf(
        section1,
        section2
    )

    val sectionDs = sparkSession.createDataset(sections,Encoders.bean(Section::class.java))

    // for each student get the sum of the mandatory subjects

    val joinedDs = studentDs.join(sectionDs,"sectionId")

//    joinedDs.show(false)

    val cols = joinedDs.columns().toSet()

    var caseExpr = "CASE "
    for(colName in cols){
        caseExpr += " WHEN x= '$colName' then cast(cast($colName as string) as int) "
    }
    caseExpr += " ELSE null"
    caseExpr += " END"

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