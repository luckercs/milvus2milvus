import Milvus2Milvus.parseCollections
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

    val t_milvusUtil = new MilvusUtil(t_uri, t_token)
    dbCollections.foreach(item => {
      var db = item.split("\\.")(0)
      val col = item.split("\\.")(1)
      if (!targetDB.trim.equals("")) {
        db = targetDB
      }

      if (t_milvusUtil.isCollectionLoaded(db, col)) {
        LOG.warn("Collection " + db + "." + col + " is already loaded in target Milvus, skip")
      } else {
        t_milvusUtil.loadCollection(db, col)
        LOG.info("Collection " + db + "." + col + " load success")
      }
    })
    LOG.info(dbCollections.size + " collections load finished")
  }
}
