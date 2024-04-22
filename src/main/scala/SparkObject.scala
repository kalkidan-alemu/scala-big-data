
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkObject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("SparkScalaApp")
      .master("local[*]")
      .getOrCreate()

    val filePath = "C:\\KalieSchool\\Bigdata-data\\mtcars.csv"
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // Use the first row as header
      .csv(filePath)

    // step 2 : create population
    def createPairRDD(): DataFrame = {
      df.select(
        col("cyl").alias("Key"),
        col("mpg").alias("Value"))

    }

    val population = createPairRDD()
    population.show()


    // step 3: compute the mean and variance
    def computeStatsParameters(): DataFrame = {

      val statsParameters = population.groupBy("Key")
        .agg(mean("Value").alias("Mean"), var_pop("Value").alias("Variance"))
        .withColumn("Mean", format_number(col("Mean"), 2))
        .withColumn("Variance", format_number(col("Variance"), 2))
      statsParameters.withColumn("Key", col("Key").cast("String")).withColumnRenamed("Key", "Category")
    }

    val statsParameters = computeStatsParameters()
    statsParameters.show()


    // step 4
    def createSample(): Array[Row] = {
      population.rdd.takeSample(withReplacement = false, num = (population.count() * 0.25).toInt)
    }

    val sample = createSample()

    def printSample(ss: Array[Row]): Unit = {
      println("Key\t\tvalue")
      println("---------------------------")
      ss.foreach(row => println(s"${row(0)}\t\t${row(1)}"))
    }

    printSample(sample)


    val sampleRDD = spark.sparkContext.parallelize(sample)

    val schema = StructType(Array(
      StructField("Key", IntegerType, nullable = true),
      StructField("Value", DoubleType, nullable = true)
    ))


    // step 5a resample data
    val resampleData: Seq[Array[Row]] = (1 to 5)
      .map { _ => sampleRDD.takeSample(withReplacement = true, num = (sampleRDD.count() * 1).toInt) }

    val sumStats = resampleData.zipWithIndex.map { case (sample, index) =>


      // step 5b
      val reSampleDF = spark.createDataFrame(spark.sparkContext.parallelize(sample), schema)


      val meanVarDf = reSampleDF.groupBy("Key")
        .agg(mean("Value").alias("Mean"), var_pop("Value").alias("Variance"))
        .withColumn("Mean", format_number(col("Mean"), 2))
        .withColumn("Variance", format_number(col("Variance"), 2))
      meanVarDf.withColumn("Key", col("Key").cast("String")).withColumnRenamed("Key", "Category")
    }

    sumStats.zipWithIndex.foreach { case (eachStats, index) =>
      println(s"Sample $index:")
      printSample(sample)
      eachStats.show()
    }

    // step 6
    val results = sumStats.reduce(_.union(_))
    results.show()
    val resultsSquared = results.withColumn("Variance_squared", pow(("Variance"), 2))
    resultsSquared.show()
    // step 5c
    val resultSums = resultsSquared.groupBy("Category").agg(sum("Mean").alias("Sum_Means"),
      sum("Variance_squared").alias("Sum_Variance"), count("Category"))


    // step 6
    resultSums.withColumn("final_mean", col("Sum_Means") / col("Category"))
      .withColumn("final_variance", col("Sum_Variance") / col("Category"))
      .select(
        col("final_mean").alias("Mean"),
        col("final_variance").alias("Variance"))
      .show()

        resultsSquared.groupBy("Category").agg(mean("Mean").alias("Mean"), mean("Variance_squared").alias("Variance"))
          .show()

    spark.stop()
  }
}
