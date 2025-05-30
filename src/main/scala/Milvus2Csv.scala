import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.mutable.ListBuffer

object Milvus2Csv {
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
    val database = commandLine.getOptionValue("database", "default")
    val collection = commandLine.getOptionValue("collection")
    val csvDir = commandLine.getOptionValue("csv", "milvus_dump")
    val delimiter = commandLine.getOptionValue("delimiter",",")
    createOrReplaceDirectory(csvDir)

    val df = spark.read.format("milvus")
      .option("uri", uri)
      .option("token", token)
      .option("database", database)
      .option("collection", collection)
      .load()
    val count = df.count()

    df.repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", delimiter)
      .mode(SaveMode.Overwrite)
      .save(csvDir + "/" + database + "__" + collection)
    LOG.info(log_flag + "Wrote " + count + " records to " + csvDir + "/" + database + "__" + collection)

    spark.close()
  }

  def createOrReplaceDirectory(path: String): Unit = {
    val dir = new File(path)

    if (dir.exists()) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          Option(file.listFiles).foreach(_.foreach(deleteRecursively))
        }
        if (!file.delete()) {
          throw new RuntimeException(s"无法删除文件: ${file.getAbsolutePath}")
        }
      }

      deleteRecursively(dir)
    }

    if (!dir.mkdirs()) {
      throw new RuntimeException(s"无法创建目录: ${dir.getAbsolutePath}")
    }
  }


  def collectCsvFiles(dir: File): List[File] = {
    val result = ListBuffer[File]()

    if (dir.isDirectory) {
      Option(dir.listFiles()).getOrElse(Array.empty).foreach { file =>
        if (file.isDirectory) {
          result ++= collectCsvFiles(file)
        } else if (file.isFile && file.getName.toLowerCase.endsWith(".csv")) {
          result += file
        }
      }
    }

    result.toList
  }

  def moveAndRenameCsvFiles(csvFiles: List[File], targetDir: File): Unit = {
    csvFiles.foreach { csvFile =>
      try {
        val parentDirName = csvFile.getParentFile.getName
        val newFileName = s"${parentDirName}.csv"
        val targetFile = new File(targetDir, newFileName)
        val uniqueTargetFile = makeFileNameUnique(targetFile)

        if (csvFile.renameTo(uniqueTargetFile)) {
          LOG.info(s"成功: 将 '${csvFile.getAbsolutePath}' 重命名并移动为 '${uniqueTargetFile.getName}'")
        } else {
          throw new RuntimeException(s"失败: 无法将 '${csvFile.getAbsolutePath}' 移动到 '${uniqueTargetFile.getName}'")
        }
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"错误: 处理文件 '${csvFile.getAbsolutePath}' 时出错: ${e.getMessage}")
      }
    }
  }

  def makeFileNameUnique(file: File): File = {
    if (!file.exists()) return file

    var counter = 1
    val name = file.getName
    val extension = if (name.contains(".")) name.substring(name.lastIndexOf(".")) else ""
    val baseName = if (name.contains(".")) name.substring(0, name.lastIndexOf(".")) else name

    var uniqueFile = new File(file.getParent, s"${baseName}_${counter}${extension}")

    while (uniqueFile.exists()) {
      counter += 1
      uniqueFile = new File(file.getParent, s"${baseName}_${counter}${extension}")
    }

    uniqueFile
  }

  def deleteSubdirectories(rootDir: File): Unit = {
    Option(rootDir.listFiles()).getOrElse(Array.empty).foreach { file =>
      if (file.isDirectory) {
        deleteDirectoryRecursively(file)
      }
    }
  }

  def deleteDirectoryRecursively(dir: File): Boolean = {
    if (dir.isDirectory) {
      Option(dir.listFiles()).getOrElse(Array.empty).foreach { file =>
        if (!deleteDirectoryRecursively(file)) {
          return false
        }
      }
    }
    dir.delete()
  }
}
