package com.poc.spark

import scopt.OptionParser

case class ParseConfig (database: String = null, table: String = null, isLoadRequested: Boolean = null, isPostValidationRequired: Boolean = false)

object ArgsProcessor extends App {

  println("******args parsing begin...******")

  val cmdParser: OptionParser[ParseConfig] = new scopt.OptionParser[ParseConfig]("SparkScoptUsage") {

    //parsing database argument
    opt[String]('i', "database").required().action((x, c) => c.copy(database = x))
      .text("database name of the external table")
      .validate(x => if (!x.isEmpty)
        success
      else
        failure("database name is empty"))

    //parsing table argument
    opt[String]('i', "table").required().action((x, c) => c.copy(table = x))
      .text("external table name")
      .validate(x => if (!x.isEmpty)
        success
      else
        failure("table name is empty"))

    //parsing  load request argument
    opt[Boolean]('i', "load_request").optional().action((x, c) => c.copy(isLoadRequested = x)).text("pass true, If you need a new file from local to inbound,when you don't have file to be processed at Inbound location")

    //parsing  high-level testing request argument
    opt[Boolean]('i', "data_validation").optional().action((x, c) => c.copy(isPostValidationRequired = x)).text("pass true, If you need a overview of current data in existing table(i.e High-level overview of data count)")
  }


}
