package org.example

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*


@NoArgEntity
data class StudentScore(
    var studentId:Int,
    var sectionId:Int,
    var name:String,
    var math:Int,
    var science:Int,
    var english:Int,
)

@NoArgEntity
data class Section(
    var sectionId:Int,
    var mandatorySubjects:List<String>
)

@NoArgEntity
data class StudentMandatorySumResult(
    var studentId:Int,
    var sectionId:Int,
    var name:String,
    var math:Int,
    var science:Int,
    var english:Int,
    var mandatorySubjectsScoreSum:Int
)

fun getStudentDs():Dataset<StudentScore>{
    val sparkSession = getSparkSession()

    val studentScore1 = StudentScore(1,100,"student1",50,60,70)
    val studentScore2 = StudentScore(2,200,"student2",30,20,80)
    val studentScore3 = StudentScore(1,100,"student3",10,60,40)

    val students = listOf(
        studentScore1,
        studentScore2,
        studentScore3
    )

    return sparkSession.createDataset(students,Encoders.bean(StudentScore::class.java))
}

fun getSectionDs():Dataset<Section>{
    val sparkSession = getSparkSession()
    val section1 = Section(100, listOf("math","english"))
    val section2 = Section(200, listOf("english","science"))

    val sections = listOf(
        section1,
        section2
    )

    return sparkSession.createDataset(sections,Encoders.bean(Section::class.java))

}

fun simpleScoreSum(studentScoreDs:Dataset<StudentScore>){
    val resultDf = studentScoreDs
        .withColumn("totalScore",
            col("english").plus(col("science")).plus(col("math")))

    /**
     * or
     * studentDs.withColumn("totalScore",expr("english + science + math"))
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
                mandatorySubjects,
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

fun mandatoryScoreSumUsingGeneratedCaseWhenCollect(joinedDs:Dataset<Row>){
    val sectionDs = getSectionDs()
    //assuming that sectionDs is small dataset
    val cols = sectionDs
        .withColumn("mandatorySubject", explode(col("mandatorySubjects")))
        .select("mandatorySubject")
        .distinct()
        .collectAsList()
        .map { it.getString(0) }
        .toSet()

    var caseExpr = "CASE \n"
    for(colName in cols){
        caseExpr += " WHEN x= '$colName' then cast($colName as int) \n"
    }
    caseExpr += " ELSE null \n"
    caseExpr += "END \n"

    println(caseExpr)

    val result = joinedDs.withColumn(
        "mandatoryScoreValues",
        expr("""
            transform(
                mandatorySubjects,
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

fun mandatorySumUsingMapFunction(){

    val studentDs = getStudentDs()
    val sectionDs = getSectionDs()

    val sectionIdToMandatoryScoresMap = sectionDs.collectAsList().associate { it.sectionId to it.mandatorySubjects }

    val resultDs = studentDs.map(
        MapFunction <StudentScore,StudentMandatorySumResult>{
            val mandatoryScores = sectionIdToMandatoryScoresMap[it.sectionId]!!
            var mandatoryScoreSum = 0
            for (mandatoryScore in mandatoryScores){
                val field = it.javaClass.getDeclaredField(mandatoryScore)
                field.trySetAccessible()
                val fieldValue = field.getInt(it)
                mandatoryScoreSum += fieldValue
            }
            StudentMandatorySumResult(
                it.studentId,
                it.sectionId,
                it.name,
                it.math,
                it.science,
                it.english,
                mandatoryScoreSum
            )
        },
        Encoders.bean(StudentMandatorySumResult::class.java)
    )

    resultDs.show(false)

}


fun main(){

    val studentDs = getStudentDs()
    val sectionDs = getSectionDs()

    simpleScoreSum(studentDs)

    val joinedDs = getJoinedDs()

    mandatoryScoreSumUsingGeneratedCaseWhen(joinedDs)

    mandatoryScoreSumUsingGeneratedCaseWhenCollect(joinedDs)
    mandatorySumUsingMapFunction()
}
