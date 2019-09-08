package Excercises.ITversity

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.concat

object GetMonthlyCrimeCountByType {

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

    def crimeParser(record:String) = {
      val x = record.split(",")
      val Date = x(2)
      val Type = x(5)
      (Date, Type)
    }

    println("==================Initial Load=====================\n")
    val raw_Crimes: RDD[String] = spark.sparkContext.textFile(inputPath)
    //Removing header
    val map_Crimes = raw_Crimes.map(x => crimeParser(x)).filter(x => x._1!="Date")
    //caching the RDD
    map_Crimes.cache()

    println("==================Monthly CrimeCount Using SPARK RDD=====================\n")
    val kv_Crimes = map_Crimes.map(s => ((s._1.substring(6,10)+s._1.substring(0,2).toInt, s._2), 1))
    val reduced_Crimes = kv_Crimes
      .reduceByKey(_+_)
      .map(x => ((x._1._1, -x._2), x._1._2))
      .sortByKey().map(x => x._1._1 + "," + -x._1._2 + "," + x._2)

    println("==================Monthly CrimeCount Result Using SPARK RDD=====================\n")
   // reduced_Crimes.take(10).foreach(println)

    println("==================Monthly CrimeCount Using SPARK DATAFRAME=====================\n")
   // val df_Crime = kv_Crimes.map(x => x._1).toDF("Date", "Type")
    //(201812,DECEPTIVE PRACTICE)

   val df_Crime = map_Crimes.toDF("Date", "Type")
    //(12/30/2018 05:44:00 PM, THEFT)

    df_Crime.printSchema()

    val final_Dataframe_df = df_Crime.withColumn("Date", concat($"Date".substr(7,4), $"Date".substr(0,2)))
      .groupBy("Date", "Type")
      .count().as("Count")
      .sort($"Date".asc, $"count".desc)

    println(" DataFrame count is " + final_Dataframe_df.count())
    final_Dataframe_df.show()

    println("==================Monthly CrimeCount Using SPARK SQL=====================\n")
    df_Crime.createOrReplaceTempView("crime_type")

    //Listing tables and printing sample data from crime_type
    spark.catalog.listTables().show()
    spark.sql("select * from crime_type limit 10").show()

    //Use substr and concat to manipulate the column
    val final_sql_df = spark.sql(
      """SELECT concat(substr(Date,7,4), substr(Date,0,2)) as Dt, Type, count(Type) as cnt FROM crime_type
        |GROUP BY Dt, Type
        |ORDER BY Dt asc, cnt desc
      """.stripMargin)

    println("SQL count is "  + final_sql_df.count())
    final_sql_df.show
  }

}
