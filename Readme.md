# POC : Spark automated incremental load . 

#### This repository contains project of  'Automated Spark incremental data ingestion' from FileSystem to HDFS.

The inbound folder will contains the input csv files. When you trigger the spark job , following steps will takes place.
  
 *  Spark will pick the latest arrived file in the inbound folder automatically and  validate,process and ingest to HDFS.
  
  
 *  During the validation, if you found that file is already loaded to HDFS, then you can request new load from spark-submit optional parameters.This optional parameters are developed by scala's scopt library.When you request a new load flag, scala script will fetch a new file from external location(as this is a poc, It is simulated as some other directory than inbound within same file system) to Inbound and load that file to HDFS table.
  
  
 *  Once the data is read and validated , it will insert into given parameterized avro table or overwrite if table already exists.
