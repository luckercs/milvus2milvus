import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable
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
    val targetDB = commandLine.getOptionValue("target_db", "")
    val collectionsSkip = commandLine.getOptionValue("skip", "")
    val t_uri = commandLine.getOptionValue("t_uri")
    val t_token = commandLine.getOptionValue("t_token", "root:Milvus")
    val batchsize = commandLine.getOptionValue("batchsize", "1000").toInt
    val skip_schema = commandLine.hasOption("skip_schema")
    val skip_index = commandLine.hasOption("skip_index")

    val dbCollections = parseCollections(uri, token, collections, collectionsSkip)
    if (!skip_schema) {
      LOG.info(log_flag + "start create target collections schemas: " + dbCollections.mkString(", "))
      destSchemaCreate(uri, token, t_uri, t_token, dbCollections, skip_index, targetDB)
    }

    LOG.info(log_flag + "start move collections data: " + dbCollections.mkString(", "))
    val collectionNumALL = dbCollections.size
    var current = 1
    dbCollections.foreach(item => {
      val db = item.split("\\.")(0)
      val col = item.split("\\.")(1)

      val df = spark.read.format("milvus")
        .option("uri", uri)
        .option("token", token)
        .option("database", db)
        .option("collection", col)
        .option("batchsize", batchsize)
        .load()
      val count = df.count()
      LOG.info(log_flag + "Read " + count + " records: " + db + "." + col)

      if (!targetDB.trim().equals("")) {
        df.write.format("milvus")
          .option("uri", t_uri)
          .option("token", t_token)
          .option("database", targetDB)
          .option("collection", col)
          .option("batchsize", batchsize)
          .mode(SaveMode.Append)
          .save()
        LOG.info(log_flag + "Wrote " + count + " records: " + targetDB + "." + col + ", progress: [" + current + "/" + collectionNumALL + "]")
      } else {
        df.write.format("milvus")
          .option("uri", t_uri)
          .option("token", t_token)
          .option("database", db)
          .option("collection", col)
          .option("batchsize", batchsize)
          .mode(SaveMode.Append)
          .save()
        LOG.info(log_flag + "Wrote " + count + " records: " + db + "." + col + ", progress: [" + current + "/" + collectionNumALL + "]")
      }
      current = current + 1
    })
    LOG.info(log_flag + " " + dbCollections.size + " collections moved successfully.")
    spark.close()
  }

  def parseCollections(uri: String, token: String, collections: String, collectionsSkip: String): List[String] = {
    val parsedCollections = ListBuffer[String]()
    collections.trim.split(",").foreach(collection => {
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

    if (!collectionsSkip.trim.equals("")) {
      val collectionsSkipLists = collectionsSkip.trim.split(",").toList
      parsedCollections --= collectionsSkipLists
    }
    parsedCollections.toList
  }

  def destSchemaCreate(uri: String, token: String, t_uri: String, t_token: String, collections: List[String], skip_index: Boolean, targetDB: String): Unit = {
    val milvusUtil = new MilvusUtil(uri, token)
    val t_milvusUtil = new MilvusUtil(t_uri, t_token)

    val t_dbs = t_milvusUtil.getAllDbs()
    if (!targetDB.trim().equals("")) {
      if (!t_dbs.contains(targetDB)) {
        t_milvusUtil.createDatabase(targetDB, null)
        LOG.info(log_flag + "target create database: " + targetDB)
      }
    } else {
      collections.map(_.split("\\.")(0)).distinct.foreach(dbname => {
        if (!dbname.equals("default") && !t_dbs.contains(dbname)) {
          t_milvusUtil.createDatabase(dbname, milvusUtil.getDataBaseSchema(dbname))
          LOG.info(log_flag + "target create database: " + dbname)
        }
      })
    }

    collections.foreach(item => {
      val db = item.split("\\.")(0)
      val col = item.split("\\.")(1)
      if (!targetDB.trim().equals("")) {
        if (t_milvusUtil.hasCollection(targetDB, col)) {
          throw new RuntimeException("target collection already exists: " + item + ", please check and drop it first.")
        }
      } else {
        if (t_milvusUtil.hasCollection(db, col)) {
          throw new RuntimeException("target collection already exists: " + item + ", please check and drop it first.")
        }
      }
    })


    if (!targetDB.trim().equals("")) {
      collections.foreach(item => {
        val db = item.split("\\.")(0)
        val col = item.split("\\.")(1)
        val milvusClient = milvusUtil.getMilvusClient(db)
        val t_milvusClient = t_milvusUtil.getMilvusClient(targetDB)
        t_milvusUtil.createCollectionFromExistsCollection(milvusClient, t_milvusClient, db, targetDB, col, skip_index)
        LOG.info(log_flag + "target create collection: " + targetDB + "." + col)
        milvusClient.close()
        t_milvusClient.close()
      })
    } else {
      collections.foreach(item => {
        val db = item.split("\\.")(0)
        val col = item.split("\\.")(1)
        val milvusClient = milvusUtil.getMilvusClient(db)
        val t_milvusClient = t_milvusUtil.getMilvusClient(db)
        t_milvusUtil.createCollectionFromExistsCollection(milvusClient, t_milvusClient, db, db, col, skip_index)
        LOG.info(log_flag + "target create collection: " + item)
        milvusClient.close()
        t_milvusClient.close()
      })
    }
  }
}
