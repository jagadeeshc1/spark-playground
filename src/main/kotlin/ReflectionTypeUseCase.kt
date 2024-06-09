package org.example

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*


/**
 *
 * this is the code for the blog: https://jagac.substack.com/p/apache-spark-reflection-type-use
 *
 *
 */

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


@NoArgEntity
data class StudentScore2(
    var studentId:Int,
    var sectionId:Int,
    var name:String,
    var scores:Map<String,Int>
)

fun getStudentScoreDs():Dataset<StudentScore>{
    val sparkSession = getSparkSession()

    val studentScore1 = StudentScore(1,100,"student1",50,60,70)
    val studentScore2 = StudentScore(2,200,"student2",30,20,80)
    val studentScore3 = StudentScore(3,100,"student3",10,60,40)

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
     * studentScoreDs.withColumn("totalScore",expr("english + science + math"))
     *
     * both are same
     */

    resultDf.show(false)

}


fun getJoinedDs():Dataset<Row>{
    val studentScoreDs = getStudentScoreDs()
    val sectionDs = getSectionDs()
    val joinedDs = studentScoreDs.join(sectionDs,"sectionId")
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

    val studentScoreDs = getStudentScoreDs()
    val sectionDs = getSectionDs()

    val sectionIdToMandatoryScoresMap = sectionDs.collectAsList().associate { it.sectionId to it.mandatorySubjects }

    val resultDs = studentScoreDs.map(
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

fun getStudentScore2Ds():Dataset<StudentScore2>{
    val sparkSession = getSparkSession()

    val studentScore1 = StudentScore2(1,100,"student1", mapOf(
        "math" to 50,
        "science" to 60,
        "english" to 70
    ))
    val studentScore2 = StudentScore2(2,200,"student2", mapOf(
        "math" to 30,
        "science" to 20,
        "english" to 80
    ))
    val studentScore3 = StudentScore2(3,100,"student3",mapOf(
        "math" to 10,
        "science" to 60,
        "english" to 40
    ))

    val students = listOf(
        studentScore1,
        studentScore2,
        studentScore3
    )

    return sparkSession.createDataset(students,Encoders.bean(StudentScore2::class.java))
}

fun mandatorySumUsingMapType(){
    val studentScore2Ds = getStudentScore2Ds()

    studentScore2Ds.show(false)

    val sectionDs = getSectionDs()

    val joinedDs = studentScore2Ds.join(sectionDs,"sectionId")

    val result = joinedDs.withColumn("mandatoryScoreValues", expr(
        """
            transform(
                mandatorySubjects,
                x->scores[x]
            )
        """.trimIndent()
    )).withColumn(
        "mandatoryScoreSum",
        expr("""
            aggregate(mandatoryScoreValues,0,(acc,x)->acc+x)
        """.trimIndent())
    )

    result.show(false)
}


fun main(){

    val studentScoreDs = getStudentScoreDs()
    val sectionDs = getSectionDs()

    simpleScoreSum(studentScoreDs)

    val joinedDs = getJoinedDs()

    mandatoryScoreSumUsingGeneratedCaseWhen(joinedDs)

    mandatoryScoreSumUsingGeneratedCaseWhenCollect(joinedDs)
    mandatorySumUsingMapFunction()
    mandatorySumUsingMapType()
}
