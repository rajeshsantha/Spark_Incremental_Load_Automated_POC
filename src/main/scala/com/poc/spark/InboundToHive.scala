package com.poc.spark

import java.io.File
import sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.poc.spark.ArgsProcessor.cmdParser
import com.poc.spark.ParseConfig
//spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 --class com.poc.spark.InboundToHive /home/rajeshs/jars_from_intellij/new_jars/poc_hivetohbase_2.11-0.1.jar rajeshs_task_db retail_invoice_incr_avro request_load test
//com.poc.spark.InboundToHive

object InboundToHive {

  val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nn01.itversity.com:8020")
  val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val hdfs_home_dir = hadoopfs.getHomeDirectory
  val inbound_location: String = hdfs_home_dir + "/external_table_location/inbound/"
  val table_location: String = hdfs_home_dir + "/external_table_location/sparkTableLoc_invoice_incr_avro"
  val localPath = System.getProperty("user.home") + "/datasets/new_datasets/definitive_guide"
  val archive_dir = System.getProperty("user.home") + "/datasets/new_datasets/archive_dir"
  var isReloaded:Boolean = false


  def main (args: Array[String]) {

    val database_name: String = args(0) //rajeshs_task_db
    val table_name: String = args(1) //retail_invoice_incr_avro

    val isTableLocationValid: Boolean = checkIfFileExists(table_location)
    val isInboundLocationValid: Boolean = checkIfFileExists(inbound_location)
    // Need to implement args parser with scopt.OParser [pending]
    if (args.length >= 2) {
      println("database_name = " + database_name)
      println("table_name = " + table_name)
    } else {
      println("terminating program since no database,table parameters didn't pass ")
      System.exit(1)
    }
    if (!isTableLocationValid) {
      println("please check if " + table_location + " is valid path")
      System.exit(1)
    } else if (!isInboundLocationValid) {
      println("please check if " + inbound_location + " is valid path")
      System.exit(1)
    }
    val isNewLoadRequested: Boolean = args.length >= 3 && args(2).equalsIgnoreCase("request_load")
    println("*****Requestd Reload ? :"+isNewLoadRequested+" ****")
    val isTableExists: Boolean = spark.catalog.tableExists(database_name, table_name)


    //hdfs dfs -cp /user/rajeshs/external_table_location/savedForLater/2011-12-08.csv /user/rajeshs/external_table_location/inbound
    val filename = getLatestFile()
    spark.sparkContext.setLogLevel("ERROR")
    val customerInvoice_DF =readDataFrame(filename)
    customerInvoice_DF.show(2, false)
    if (isTableExists) {
      val existing_data_df = spark.sql("select * from " + database_name + "." + table_name)
      if (isFileAlreadyProcessed(filename, existing_data_df)) {
        println("file is already processed... ")
        if (!isNewLoadRequested) {
          println(filename + " is already loaded to table \n Please add optional parameter 'request_load' at the end of spark-submit to load the new file to inbound")
          println("current status of data is")
          postValidation(database_name + "." + table_name)
        }
        else {
          println(" Load requested with new file from local to inbound")
          val localFilePath = localPath + "/" + getLatestLocalFile(localPath)
          val returncode = fileTransferToInbound(localFilePath, inbound_location)
          if(returncode==0) println("file backup is successful") else println("file backup is failed")
          println("reloading inbound with new file")
          val filename = getLatestFile()
          val customerInvoice_DF_new =readDataFrame(filename)
          isReloaded=true;
          //println("isReloaded = "+isReloaded)
          customerInvoice_DF_new.show(1,false)
          println("Inserting new data to table")
          writeToTable(database_name,table_name,customerInvoice_DF_new,isTableExists)
          println("new data insertion is completed")
        }
      }
    }
   if(!isReloaded) {
     println("Not requested reload, isReload is "+isReloaded)
     println("calling regular writeToTable")
     writeToTable(database_name,table_name,customerInvoice_DF,isTableExists)
   }
//if request_load added as arg(2)
    if (args.length > 3 && args(3).equalsIgnoreCase("test")) postValidation(database_name + "." + table_name)
//if request_load ignored as arg(2)
    if (args.length == 3 && args(2).equalsIgnoreCase("test")) postValidation(database_name + "." + table_name)


  }
  def readDataFrame(filename:String): DataFrame =
  {
    println("filename for reading dataframe "+filename)
    val customerInvoice_DF_raw = spark.read.format("csv").option("header", "true").load(inbound_location + filename)
    val customerInvoice_DF = customerInvoice_DF_raw.withColumn("datestr", regexp_replace(to_date(col("InvoiceDate")).cast("String"), "-", "").cast("int"))
    customerInvoice_DF
  }

  /**
    *
    * @param path
    * @return whehter mentioned hdfs path exists
    */
  def checkIfFileExists (path: String): Boolean = {
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  /**
    *
    * @returns latest file available in hdfs inbound, for processing and loading
    */
  def getLatestFile (): String = {
    val status = hadoopfs.listStatus(new Path(inbound_location))
    println(" The available files at inbound location are")
    status.filter(_.isFile).foreach(x => println(x.getPath.getName))
    println("picking latest file from inbound")
    val availalbe_files = status.filter(_.isFile).map(x => x.getPath.getName)
    val filename = availalbe_files.last
    println("latest file name : " + filename)
    filename
  }

  /**
    *
    * @param table
    * @returns nothing. Intended for post data load validation(optionally parameterized) and terminates the program with return 0
    */
  def postValidation (table: String) = {
    val new_data_df = spark.sql("select * from " + table)
    new_data_df.groupBy("datestr").count.show
    println("total number of records :" + new_data_df.count)
    println("total number of unique records :" + new_data_df.distinct.count)
    println("******job is completed******")
    System.exit(0)
  }

  /**
    *
    * @param file
    * @param df
    * @return true, if the file which passed in already processed and loaded to table
    */
  def isFileAlreadyProcessed (file: String, df: DataFrame): Boolean = {
    val current_partition = df.select("datestr").distinct.sort(expr("datestr").desc).first.getInt(0)
    val fileValue = file.replaceAll("-", "").split('.')(0).toInt
    println("current_partition "+current_partition+" and fileValue "+fileValue)
if(current_partition == fileValue) println("file was already processed") else println("new file")
    current_partition == fileValue
  }

  /**
    *
    * @param local_file_path
    * @param destFilePath
    * @return status code 0 or 1
    */
  def fileTransferToInbound (local_file_path: String, destFilePath: String) = {
    val srcPath = new Path(local_file_path)
    val destPath = new Path(destFilePath)

    hadoopfs.copyFromLocalFile(srcPath, destPath)
    println("file copied to inbound location")

  "mv -i " + local_file_path + " " + archive_dir + "" !
  }

  /**
    *
    * @param Local directory name
    * @returns latest local file available for process
    */
  def getLatestLocalFile (dir: String) ={
    val dir = new File(localPath)
    val local_filename = dir.listFiles.filter(_.isFile).map(_.getName).sorted.head
    println("latest local filename : "+local_filename)
    local_filename
  }

  /**
    *
    * @param database_name
    * @param table_name
    * @param customerInvoice_DF
    * @param isTableExists
    *
    * @returns nothing.
    *         Write the given dataframe to avro partitioned external table
    *
    */
  def writeToTable(database_name:String,table_name:String,customerInvoice_DF:DataFrame,isTableExists:Boolean)={
  if (isTableExists) {
    println(database_name + "." + table_name + " is exists")
    val existing_data_df = spark.sql("select * from " + database_name + "." + table_name)
    existing_data_df.groupBy("datestr").count.show
    spark.sql("use " + database_name)
    println("inserting into " + table_name)
    println("records to be added to existing table is " + customerInvoice_DF.count)
    val current_partition = customerInvoice_DF.select("datestr").limit(1).first.getInt(0)
    customerInvoice_DF.write.format("com.databricks.spark.avro").option("path", table_location+"/datestr="+current_partition).insertInto(table_name)
    println("insertion is completed")
  }
  else {
    println(database_name + "." + table_name + " doesn't exists \n creating " + table_name)
    spark.sql("use " + database_name)
    println("records to be added to new table is " + customerInvoice_DF.count)
    customerInvoice_DF.write.mode("overwrite").format("com.databricks.spark.avro").partitionBy("datestr").option("path", table_location).saveAsTable(table_name)
    println(table_name + " is created")
  }
}
  def argParsing(args:Array[String])={

    cmdParser.parse(args, ParseConfig() ) map { config =>
      // do stuff
      val database_name:String = config.database
      val table_name:String = config.table
      val isNewLoadRequested: Boolean =config.isLoadRequested
      println("database_name==" + database_name)
      println("database_name=" + table_name)
      println("isNewLoadRequested=" + isNewLoadRequested)

    } getOrElse {
      // arguments are bad, usage message will have been displayed
    }
    val database_name: String = database_name //rajeshs_task_db
    val table_name: String = table_name //retail_invoice_incr_avro

    val isTableLocationValid: Boolean = checkIfFileExists(table_location)
    val isInboundLocationValid: Boolean = checkIfFileExists(inbound_location)
    // Need to implement args parser with scopt.OParser [pending]
    if (args.length >= 2) {
      println("database_name = " + database_name)
      println("table_name = " + table_name)
    } else {
      println("terminating program since no database,table parameters didn't pass ")
      System.exit(1)
    }
    if (!isTableLocationValid) {
      println("please check if " + table_location + " is valid path")
      System.exit(1)
    } else if (!isInboundLocationValid) {
      println("please check if " + inbound_location + " is valid path")
      System.exit(1)
    }
    val isNewLoadRequested: Boolean = args.length >= 3 && args(2).equalsIgnoreCase("request_load")
    println("*****Requestd Reload ? :"+isNewLoadRequested+" ****")
  }
}

/*    val customerInvoiceSchema = StructType(Array(
      StructField("InvoiceNo", IntegerType, true),
      StructField("StockCode", IntegerType, true),
      StructField("Description", IntegerType, true),
      StructField("InvoiceDate", IntegerType, true),
      StructField("UnitPrice", IntegerType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("Country", IntegerType, true)))

        case class CustomerInvoice (InvoiceNo: Integer,
// StockCode:Integer,
// Description:String,
// Quantity:Integer,
// InvoiceDate:Date,
// UnitPrice:Double,
// CustomerID:Double,
// Country:String)
// datestr is partition column


*/


