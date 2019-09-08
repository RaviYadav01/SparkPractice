package Excercises.ITversity

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GetTop3CrimesinRESIDENCE {

  def main(args: Array[String]): Unit = {
    val masterOfCluster = "local"
    val inputPath = "D:\\DATA\\data\\Crimes.txt"

    val spark = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    def crimeParser(record: String) = {
      val x = record.split(",")
      val Type = x(5)
      val location = x(7)
      (Type, location)
    }

    println("==================Initial Load=====================\n")
    val raw_Crimes: RDD[String] = spark.sparkContext.textFile(inputPath)
    //Removing header
    val map_Crimes = raw_Crimes.map(x => crimeParser(x)).filter(x => x._1!="Primary Type")
    //caching the RDD
    //(Type, Location)

    map_Crimes.cache()
    map_Crimes.take(10).foreach(println)

    println("==================Processing using RDDs=====================\n")
    val residance_rdd = map_Crimes
      .filter(x => x._2 =="RESIDENCE")
      .map(x => (x._1,1))
      .reduceByKey(_+_)
      .map(x =>(x._2, x._1)).sortByKey(false).take(3)

    residance_rdd.foreach(println)

    println("==================Processing using DataFrame=====================\n")
    val residance_df = map_Crimes.toDF("Type", "Location")
      .filter($"Location" === "RESIDENCE")
      .groupBy($"Type")
      .count().as("count")
      .sort($"count".desc).limit(3)

    residance_df.explain()
    residance_df.show()

    println("==================Processing using SPARK SQL=====================\n")
    val residence_sql_df = map_Crimes.toDF("Type", "Location")
    residence_sql_df.createOrReplaceTempView("crime_residence")

    spark.catalog.listTables().show()
    val residence_sql_final = spark.sql(
      """SELECT Type, count(Type) as cnt FROM crime_residence
         WHERE Location == "RESIDENCE"
         GROUP BY Type
         ORDER BY cnt desc LIMIT 3
      """.stripMargin)

    residence_sql_final.explain()
    residence_sql_final.show
  }
}
