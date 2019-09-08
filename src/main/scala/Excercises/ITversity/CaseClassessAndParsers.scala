package Excercises.ITversity

object CaseClassessAndParsers {

  case class orders(order_id:Int, order_date:String, order_customer_id:Int, order_status:String)
  case class customers(customer_id:Int, customer_fname:String, customer_lname:String, customer_email:String, customer_password:String, customer_street:String, customer_city:String, cutomer_stae:String, customer_zipcode:String)
  case class ordersItems(order_item_id:Int, order_item_order_id:Int, order_item_product_id:Int, order_item_quantity:Int, order_item_subtotal:Double, order_item_product_price:Double)
  case class products(product_id:Int, product_category_id:String, product_name:String,product_description:String, product_price:Float, product_image:String)
  case class categories(category_id:Int, category_department_id:Int, category_name:String)
  case class departments(department_id:Int, department_name:String)

  def orderParser(record:String) = {
    val field = record.split(",")
    val order_id = field(0).toInt
    val order_date = field(1)
    val order_customer_id = field(2).toInt
    val order_status = field(3)
    orders(order_id,order_date,order_customer_id, order_status)
  }

  def customerParser(record:String) = {
    val field = record.split(",")
    val id = field(0).toInt
    val fname = field(1)
    val lname = field(2)
    val email = field(3)
    val password = field(4)
    val street = field(5)
    val city = field(6)
    val state = field(7)
    val  zipcode = field(8)
    customers(id, fname,lname,email,password,street,city,state,zipcode)
  }
  def orderitemParser(record: String) = {
    val field = record.split(",")
    val id = field(0).toInt
    val oi_order_id = field(1).toInt
    val oi_pid = field(2).toInt
    val oi_quantity = field(3).toInt
    val oi_subtotal = field(4).toDouble
    val oi_product_price = field(5).toDouble
    ordersItems(id, oi_order_id, oi_pid, oi_quantity, oi_subtotal, oi_product_price)
  }

  def productParser(record:String) = {
    val p = record.split(",")

    val pid = p(0).toInt
    val pcid = p(1)
    val pname = p(2)
    val pdes = p(3)
    val pprice= p(4).toFloat
    val pimage = p(5)
    products(pid, pcid, pname, pdes, pprice, pimage)
  }

  def categoryParser(record:String) = {
    val c = record.split(",")

    val cid = c(0).toInt
    val c_did = c(1).toInt
    val c_name = c(2)

    categories(cid, c_did, c_name)
  }

  def departmentParser(record:String) ={
    val d = record.split(",")

    val did = d(0).toInt
    val dname = d(1)

    departments(did, dname)
  }

}
