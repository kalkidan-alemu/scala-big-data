
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.ConsoleTable.schema
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
//import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, format_number, mean, var_pop}
import org.apache.spark.sql.types.StructType

import scala.collection.IterableOnce.iterableOnceExtensionMethods

object SparkObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkScalaApp")
      .master("local[*]")
      .getOrCreate()

    val filePath = "C:\\KalieSchool\\Bigdata-data\\mtcars.csv"
    val df : DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema","true")// Use the first row as header
      .csv(filePath)



    val keyValDF = df.select(
      col("cyl").alias("Key"),
      col("mpg").alias("Value"))


       //  keyValDF.show()

    val meanDF = keyValDF.groupBy("Key")
      .agg(mean("Value").alias("Mean"),var_pop("Value").alias("Variance"))
      .withColumn("Mean",format_number(col("Mean"), 2 ))
      .withColumn("Variance",format_number(col("Variance"), 2 ))
    val meanDFWithKey = meanDF.withColumn("Key", col("Key").cast("String")).withColumnRenamed("Key", "Category")


   // meanDFWithKey.show()

   //meanDF.show()

    val sampleDF = keyValDF.rdd.takeSample(withReplacement = false,num =(keyValDF.count() * 0.25).toInt)
    //sampleDF.foreach(row =>println(row.mkString(", ")))
   // sampleDf.foreach(row => println(s"Key: ${row(0)}, Value: ${row(1)}"))
   // sampleDF.foreach(row => println(row))

    val sampleRDD = spark.sparkContext.parallelize(sampleDF)
    val sampleDFs : Seq[Array[Row]] = (1 to 5)
    .map{_ =>sampleRDD.takeSample(withReplacement = true,num =(sampleRDD.count() * 1).toInt) }

//   sampleDFs.zipWithIndex.foreach { case (sample, index) =>
//     println(s"Sample $index:")
//     sample.foreach(row => println(row))
//     println("=================")
//   }
       val schema = StructType(Array(
          StructField("Key", IntegerType, nullable = true),
          StructField("Value", DoubleType, nullable = true)
      ))
      var sumMean :Double = 0.0
      var sumVariantSquare :Double = 0.0

    sampleDFs.foreach { sample =>
        val reSampleDF =spark.createDataFrame(spark.sparkContext.parallelize(sample), schema)
        reSampleDF.filter(col("Key").isNotNull && col("Value").isNotNull)

      val meanVarDf = reSampleDF.groupBy("Key")
          .agg(mean("Value").alias("Mean"), var_pop("Value").alias("Variance"))
          .withColumn("Mean", format_number(col("Mean"), 2))
          .withColumn("Variance", format_number(col("Variance"), 2))

     // val meanVarDfWithCat = meanVarDf.withColumn("Key", col("Key").cast("String")).withColumnRenamed("Key", "Category")
        meanVarDf.select(col("Variance")).show()
       val variantSquared = meanVarDf.withColumn("VariantSquared",col("Variance") * col("Variance"))
        variantSquared.select(("VariantSquared")).show()

        sumMean += meanVarDf.agg(functions.sum("Mean").cast(DoubleType)).first().getDouble(0)

        sumVariantSquare += variantSquared.agg(sum("VariantSquared").cast(DoubleType)).first().getDouble(0)
        //println(sumMean)
        //println("=================")
       // println(sumVariantSquare)
      //  println("=================")
      }
     // println(sumMean/1000)
     // println(sumVariantSquare/1000)



     spark.stop()
  }
}
