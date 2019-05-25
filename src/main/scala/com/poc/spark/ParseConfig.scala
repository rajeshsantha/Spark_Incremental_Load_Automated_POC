package com.poc.spark


case class ParseConfig
(database: String = null,
 table: String = null,
 isLoadRequested: String = null,
 isPostValidationRequired: String = null)

// --database db --table tb --load_request false --data_validation true
