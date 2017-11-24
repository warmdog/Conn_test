//import org.apache
import java.io.File

import org.apache.hadoop.yarn.state.Graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
case class Att(phone_id:Long)

object Connect {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
    val filePath = "/app/user/data/deeplearning/t_user_relation_phone/dt=20171008/000*"
    val spark = SparkSession.builder().appName("Spark Hive").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    //支持通配符路径，支持压缩文件读取
    val fileRdd = spark.sparkContext.textFile(filePath)
    ///必须在session之后 初始化
    import  spark.implicits._
    val rdd1 = fileRdd.map(x =>{
      val a = x.trim.split("\\s+")
      a(0).toLong
    })toDS()
    val rdd2 = fileRdd.map(x =>{
      val a = x.trim.split("\\s+")
      a(1).toLong
    })toDS()
    val regex = "[0-9]".r
    //选出 xyqb用户
    //val count:Dataset[Row] = spark.sql("select phone_id from xyqb.user where dt = '20171025'")
    // 选出 黑名单用户
    val count: Dataset[Row] = spark.sql("select phone from blacklist.black_type_list_new")
    val countDataset: Dataset[Long] = count.map(x => {
      //获取多行数据方法 val value = x.getAs[String](1)
      val y = (regex findAllIn x.toString()).mkString("").toLong
      y
    })
    val count1: Long = countDataset.intersect(rdd1.union(rdd2)).count()
    //spark.sqlContext.
    println(count1)
    Logger.getLogger(this.getClass).info(s"xxxxxxxxxxxxx'${count1}'")
  }


}

