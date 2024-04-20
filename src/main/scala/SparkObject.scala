
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.ConsoleTable.schema
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


    //      keyValDF.show()

    val meanDF = keyValDF.groupBy("Key")
      .agg(mean("value").alias("Mean"),var_pop("value").alias("Variance"))
      .withColumn("Mean",format_number(col("Mean"), 2 ))
      .withColumn("Variance",format_number(col("Variance"), 2 ))


    val seed = 12345
    val sampleDF = keyValDF.rdd.takeSample(withReplacement = false,num =(keyValDF.count() * 0.25).toInt)
    //sampleDf.foreach(row =>println(row.mkString(", ")))
   // sampleDf.foreach(row => println(s"Key: ${row(0)}, Value: ${row(1)}"))
    //sampleDF.foreach(row => println(row))

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
      sampleDFs.foreach { sample =>
        //sample.foreach(row => println(row))
        val reSampleDF =spark.createDataFrame(spark.sparkContext.parallelize(sample), schema)
        //reSampleDF.show()
        reSampleDF.filter(col("Key").isNotNull && col("Value").isNotNull)

        reSampleDF.groupBy("Key")
          .agg(mean("Value").alias("Mean"), var_pop("Value").alias("Variance"))
          .withColumn("Mean", format_number(col("Mean"), 2))
          .withColumn("Variance", format_number(col("Variance"), 2))
          .show()

        println("=================")
        println("=================")
      }




     spark.stop()
  }
}
