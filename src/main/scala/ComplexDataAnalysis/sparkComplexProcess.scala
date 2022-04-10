package ComplexDataAnalysis


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
object sparkComplexProcess {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("ComplexData")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    println("=====Coding start=====")

    val urldata = "https://randomuser.me/api/0.8/?results=10"
    val data = scala.io.Source.fromURL(urldata).mkString
    val rdd = spark.sparkContext.parallelize(List(data))
    val webdf = spark.read.json(rdd).toDF()
    webdf.show(10)

    webdf.printSchema()

    println("====Array to struct =========")

    val explodedf = webdf.select("nationality","results","seed","version")
    .withColumn("results",expr("explode(results)"))

    explodedf.printSchema()

    println("==========struct to string=============")

    val flattendf = explodedf.select(
      col("nationality"),
      col("results.user.cell"),
      col("results.user.dob"),
      col("results.user.email"),
      col("results.user.gender"),
      col("results.user.location.*"),
      col("results.user.md5"),
      col("results.user.name.*"),
      col("results.user.password"),
      col("results.user.phone"),
      col("results.user.picture.*"),
      col("results.user.registered"),
      col("results.user.salt"),
      col("results.user.sha1"),
      col("results.user.sha256"),
      col("results.user.username"),
      col("seed"),
      col("version")
    )
    flattendf.show(5)
    flattendf.printSchema()

    println("============XML DATA PROCESS ============")

    val xmldf = spark.read.format("com.databricks.spark.xml")
      .option("rowTag","POSLog").load("file:///D://Practice/transactions.xml")
    xmldf.show(10)
    xmldf.printSchema()

  }

}
