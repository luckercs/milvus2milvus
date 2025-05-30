import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

object Milvus2Milvus {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val log_flag = "======================="

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    val spark = SparkSession.builder.appName("Nebula2Csv").master("local[*]").config(sparkConf).getOrCreate()

    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val uri = commandLine.getOptionValue("uri", "http://localhost:19530")
    val token = commandLine.getOptionValue("token", "root:Milvus")
    val collections = commandLine.getOptionValue("collections", "*")
    val t_uri = commandLine.getOptionValue("t_uri")
    val t_token = commandLine.getOptionValue("t_token", "root:Milvus")

    val dbCollections = parseCollections(uri, token, collections)
    LOG.info(log_flag + "start move collections schemas: " + dbCollections.mkString(", "))
    destSchemaCreate(uri, token, t_uri, t_token, dbCollections)
    LOG.info(log_flag + "start move collections data: " + dbCollections.mkString(", "))

    dbCollections.foreach(item => {
      val db = item.split("\\.")(0)
      val col = item.split("\\.")(1)

      val df = spark.read.format("milvus")
        .option("uri", uri)
        .option("token", token)
        .option("database", db)
        .option("collection", col)
        .load()
      val count = df.count()

      df.write.format("milvus")
        .option("uri", t_uri)
        .option("token", t_token)
        .option("database", db)
        .option("collection", col)
        .mode(SaveMode.Overwrite)
        .save()
      LOG.info(log_flag + "Wrote " + count + " records: " + db + "." + col)
    })

    spark.close()
  }

  def parseCollections(uri: String, token: String, collections: String): List[String] = {
    val parsedCollections = ListBuffer[String]()
    collections.split(",").foreach(collection => {
      if (collection.trim().contains(".")) {
        if (collection.trim().split("\\.").length != 2) {
          throw new IllegalArgumentException("Invalid collections param: " + collection)
        }
        val dbname = collection.trim().split("\\.")(0)
        val _cname = collection.trim().split("\\.")(1)
        if (_cname.equals("*")) {
          val milvusUtil = new MilvusUtil(uri, token)
          milvusUtil.getAllCollections(dbname).asScala.foreach(col => {
            parsedCollections += dbname + "." + col
          })
        } else {
          parsedCollections += dbname + "." + _cname
        }
      } else {
        if (collection.trim().equals("*")) {
          val milvusUtil = new MilvusUtil(uri, token)
          milvusUtil.getAllDbs().asScala.foreach(dbname => {
            milvusUtil.getAllCollections(dbname).asScala.foreach(col => {
              parsedCollections += dbname + "." + col
            })
          })
        } else {
          if (collection.trim().length == 0) {
            throw new IllegalArgumentException("Invalid collections param: " + collection)
          }
          parsedCollections += "default." + collection.trim()
        }
      }
    })
    parsedCollections.toList
  }

  def destSchemaCreate(uri: String, token: String, t_uri: String, t_token: String, collections: List[String]): Unit = {
    val milvusUtil = new MilvusUtil(uri, token)
    val t_milvusUtil = new MilvusUtil(t_uri, t_token)

    val t_dbs = t_milvusUtil.getAllDbs()
    collections.map(_.split("\\.")(0)).distinct.foreach(dbname => {
      if (!dbname.equals("default") && !t_dbs.contains(dbname)) {
        t_milvusUtil.createDatabase(dbname, milvusUtil.getDataBaseSchema(dbname))
        LOG.info(log_flag + "target create database: " + dbname)
      }
    })

    collections.foreach(item => {
      val db = item.split("\\.")(0)
      val col = item.split("\\.")(1)
      if (t_milvusUtil.hasCollection(db, col)) {
        throw new RuntimeException("target collection already exists: " + item + ", please check and drop it first.")
      }
    })

    collections.foreach(item => {
      val db = item.split("\\.")(0)
      val col = item.split("\\.")(1)
      val milvusClient = milvusUtil.getMilvusClient(db)
      val t_milvusClient = t_milvusUtil.getMilvusClient(db)
      t_milvusUtil.createCollectionFromExistsCollection(milvusClient, t_milvusClient, db, col)
      LOG.info(log_flag + "target create collection: " + item)
      milvusClient.close()
      t_milvusClient.close()
    })
  }
}
