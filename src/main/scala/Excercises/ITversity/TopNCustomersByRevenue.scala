package Excercises.ITversity

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat,sum, rank, dense_rank, _}

object TopNCustomersByRevenue {

  def main(args: Array[String]): Unit = {
    val masterOfCluster = "local"
    val customerPath = "D:\\DATA\\data\\data-master\\retail_db\\customers\\part-00000.txt"
    val ordersPath = "D:\\DATA\\data\\data-master\\retail_db\\orders\\part-00000.txt"
    val ordersitemsPath = "D:\\DATA\\data\\data-master\\retail_db\\order_items\\part-00000.txt"

    // val inputPath = "D:\\DATA\\data\\transactions.txt"

    val spark = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "20")

    import spark.implicits._

    println("\n==================Initial Load=====================\n")
    val rawOrdersRDD: RDD[String] = spark.sparkContext.textFile(ordersPath)
    println("RawOrders RDD is loaded....")
    val rawCustomersRDD: RDD[String] = spark.sparkContext.textFile(customerPath)
    println("RawCustomers RDD is loaded....")
    val rawOrderItemsRDD: RDD[String] = spark.sparkContext.textFile(ordersitemsPath)
    println("RawOrderItems RDD is loaded....")

    println("\n==================Preview both RDDs=====================\n")
        rawOrdersRDD.take(10).foreach(println)
        rawCustomersRDD.take(10).foreach(println)
        rawOrderItemsRDD.take(10).foreach(println)


    println("\n==================Processing using RDDs=====================\n")

    val orders_map = rawOrdersRDD.map(x => CaseClassessAndParsers.orderParser(x))
    val customers_map = rawCustomersRDD.map(x => CaseClassessAndParsers.customerParser(x))
    val orderItem_map = rawOrderItemsRDD.map(x => CaseClassessAndParsers.orderitemParser(x))

    //caching these rdds because we use this RDD frequently going forward
    orders_map.cache()
    customers_map.cache()
    orderItem_map.cache()

    //Convrted each RDD to their respective KEY VALUE pair
    val customer_rdd = customers_map.map(c => (c.customer_id, (c.customer_fname, c.customer_lname)))
    val orders_rdd = orders_map.map(o => (o.order_id, (o.order_customer_id, o.order_date)))
    val orderitems_rdd = orderItem_map.map(oi => (oi.order_item_order_id, (oi.order_item_id, oi.order_item_subtotal)))

    //Joining Orderitems with Order table and reducing it by month
    val oi_join: RDD[(String, Iterable[(Int, Double)])] = orderitems_rdd.join(orders_rdd)
      .map(x => ((x._2._2._2.substring(0, 4) + x._2._2._2.substring(5, 7), x._2._2._1), x._2._1._2))
      // .filter(x => x._1._1 == "201307")
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()


    //Defined a fuction to get topN customers for each month, which will return List of tuple for each KEY
    def topNcustomer(t: (String, Iterable[(Int, Double)]), n: Int) = {
      val revenue: Seq[(Int, Double)] = t._2.toList.sortBy(x => -x._2)
      val leastvalue = revenue.map(x => x._2).distinct.take(n).min
      val topN: Seq[(Int, Double)] = revenue.takeWhile(x => x._2 >= leastvalue)
      topN
    }

    // oi_join.foreach(println)
    //Applying toNcustomer function and applied FlatMap to map List of Values to Key and sorted Month ascending and Amount desc
    val topNcustomers_rdd: RDD[(String, (Int, Double))] = oi_join.flatMap(rec => topNcustomer(rec, 5).map(a => (rec._1, a)))
    val join_customer_topN: RDD[String] = topNcustomers_rdd.map(x => (x._2._1, (x._1, x._2._2))).join(customer_rdd).sortBy(x => (x._2._1._1, -x._2._1._2))
      .map(x => x._2._2._1 + "," + x._2._2._2 + "," + x._2._1._1 + "," + x._2._1._2)

    println("TopN customers are : \n")
   // join_customer_topN.take(10).foreach(println)

    println("\n==================Processing using DATAFRAMES=====================\n")

    val orders_df = orders_map.toDF()
    val orderItems_df = orderItem_map.toDF()
    val customer_df = customers_map.toDF()

//        orders_df.printSchema()
//        orderItems_df.printSchema()
//        customer_df.printSchema()
//
//        orders_df.show()
//        orderItems_df.show()
//        customer_df.show()

    val oi_join_df = orderItems_df.join(orders_df, $"order_item_order_id" === $"order_id")
      .withColumn("order_date", concat($"order_date".substr(0, 4), $"order_date".substr(6, 2)))
     // .filter($"order_date" === "201307")
      .groupBy("order_date", "order_customer_id")
      .agg(sum($"order_item_subtotal").as("total_revenue"))
   //   .orderBy($"order_date".asc, $"total_revenue".desc)
      .join(customer_df, $"order_customer_id" === $"customer_id")
      .select($"order_date", $"order_customer_id", $"total_revenue", $"customer_fname", $"customer_lname")

    // Defining a WINDOW function on order_date so that we can perform any operation particular to that window
    val window = Window.partitionBy("order_date").orderBy($"total_revenue".desc)

    val topNcustomer_DF = oi_join_df.withColumn("rank", rank.over(window))
      .withColumn("dense_rank", dense_rank().over(window))
      .orderBy($"order_date".asc, $"total_revenue".desc)
      .filter($"rank" <= 5)

    topNcustomer_DF.show(20)

    println("\n==================Processing using SPARK SQL=====================\n")

    spark.catalog.listTables().show()

    orders_df.createOrReplaceTempView("orders")
    orderItems_df.createOrReplaceTempView("orderItems")
    customer_df.createOrReplaceTempView("customers")

    spark.catalog.listTables().show()

    val topNcustomer_SQL = spark.sql(
      """
        |SELECT * FROM
        |(SELECT  a.order_date, a.order_customer_id, a.total_revenue, dense_rank() over(PARTITION BY a.order_date ORDER BY a.total_revenue desc) as denseRank FROM
        |(SELECT cast(concat(substr(o.order_date,0,4), substr(o.order_date, 6,2)) as int) as order_date, o.order_customer_id, round(sum(oi.order_item_subtotal),2) as total_revenue
        |FROM orders o JOIN orderItems oi ON o.order_id = oi.order_item_order_id
        |GROUP BY order_date, o.order_customer_id) a) b
        |WHERE b.denseRank < 6
        |ORDER BY b.order_date, b.total_revenue desc
      """.stripMargin)

    topNcustomer_SQL.printSchema()
    topNcustomer_SQL.show

  }
}
