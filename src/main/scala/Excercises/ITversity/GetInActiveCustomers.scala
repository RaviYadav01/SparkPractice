package Excercises.ITversity

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GetInActiveCustomers {

  def main(args: Array[String]): Unit = {
    val masterOfCluster = "local"
    val ordersPath = "D:\\DATA\\data\\data-master\\retail_db\\orders\\part-00000.txt"
    val customerPath = "D:\\DATA\\data\\data-master\\retail_db\\customers\\part-00000.txt"
   // val inputPath = "D:\\DATA\\data\\transactions.txt"

    val spark = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("\n==================Initial Load=====================\n")
    val rawOrdersRDD: RDD[String] = spark.sparkContext.textFile(ordersPath)
    println("RawOrders RDD is loaded....")
    val rawCustomersRDD: RDD[String] = spark.sparkContext.textFile(customerPath)
    println("RawCustomers RDD is loaded....")

    println("\n==================Preview both RDDs=====================\n")
    rawOrdersRDD.take(10).foreach(println)
    rawCustomersRDD.take(10).foreach(println)


    println("\n==================Processing using RDDs=====================\n")

    val orders_map = rawOrdersRDD.map(x => CaseClassessAndParsers.orderParser(x))
    val customers_map = rawCustomersRDD.map(x => CaseClassessAndParsers.customerParser(x))

    //caching these rdds because we use this RDD frequently going forward
    orders_map.cache()
    customers_map.cache()

    val orders_rdd = orders_map.map(o => (o.order_customer_id, o.order_id))
    val customers_rdd = customers_map.map(c => (c.customer_id, c.customer_fname))
    orders_rdd.take(10).foreach(println)
    customers_rdd.take(10).foreach(println)

    val joined_rdd: RDD[(Int, (String, Option[Int]))] = customers_rdd.leftOuterJoin(orders_rdd).filter(_._2._2 == None)//.map(x => x._1 + "," + x._2._1)
    println("Numner of inactive customers using RDD are " + joined_rdd.count())
    joined_rdd.take(10).foreach(println)

    println("\n==================Processing using DATAFRAMES=====================\n")

    val orders_DF = orders_map.toDF()
    val customer_DF = customers_map.toDF()

    orders_DF.printSchema()
    customer_DF.printSchema()

    println("\n orders count is " + orders_DF.count())
    println("\n customers count is"  + customer_DF.count())

    orders_DF.show()
    customer_DF.show

    val joined_DF = customer_DF.join(orders_DF, customer_DF("customer_id") === orders_DF("order_customer_id"), "left")
      .select("customer_id", "customer_fname","customer_lname" )
        .filter($"order_id".isNull)

    println("Inactive customers using DataFrame: " + joined_DF.count())
    joined_DF.show(false)

    println("\n==================Processing using SPARK SQL=====================\n")

    orders_DF.createOrReplaceTempView("orders")
    customer_DF.createOrReplaceTempView("customers")

    spark.catalog.listTables().show()

    val joined_DF_SQL= spark.sql(
      """
        |SELECT c.customer_id , c.customer_fname, c.customer_lname FROM customers c LEFT OUTER JOIN orders o ON c.customer_id == o.order_customer_id
        |WHERE o.order_id is null
      """.stripMargin)

    println("Inactive customers using DataFrame: " + joined_DF_SQL.count())
    joined_DF_SQL.show(false)


  }

}
