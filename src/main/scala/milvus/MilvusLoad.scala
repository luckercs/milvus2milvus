package milvus

import milvus.Milvus2Milvus.parseCollections
import org.slf4j.LoggerFactory

object MilvusLoad {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val optionsProcessor = new OptionsProcessor()
    val commandLine = optionsProcessor.parse(args)
    val uri = commandLine.getOptionValue("uri", "http://localhost:19530")
    val token = commandLine.getOptionValue("token", "root:Milvus")
    val collections = commandLine.getOptionValue("collections", "*")
    val targetDB = commandLine.getOptionValue("target_db", "")
    val collectionsSkip = commandLine.getOptionValue("skip", "")
    val t_uri = commandLine.getOptionValue("t_uri")
    val t_token = commandLine.getOptionValue("t_token", "root:Milvus")

    val dbCollections = parseCollections(uri, token, collections, collectionsSkip)

    val collectionNumALL = dbCollections.size
    var current = 1
    val t_milvusUtil = new MilvusUtil(t_uri, t_token)
    dbCollections.foreach(item => {
      val db_src = item.split("\\.")(0)
      var db = db_src
      val col = item.split("\\.")(1)
      if (!targetDB.trim.equals("")) {
        db = targetDB
      }

      if (t_milvusUtil.isCollectionLoaded(db, col)) {
        LOG.warn("Collection " + db + "." + col + " is already loaded in target Milvus, skip it" + ", progress: [" + current + "/" + collectionNumALL + "]")
      } else {
        try {
          t_milvusUtil.loadCollection(db, col)
          LOG.info("Collection " + db + "." + col + " load success" + ", progress: [" + current + "/" + collectionNumALL + "]")
        } catch {
          case e: Exception => {
            LOG.error("Collection " + db + "." + col + " load failed, src: [" + db_src + "." + col + "], error: " + e.getMessage)
            throw e
          }
        }
      }
      current = current + 1
    })
    LOG.info(dbCollections.size + " collections load finished")
  }
}
