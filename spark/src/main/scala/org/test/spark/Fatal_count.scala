package org.test.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object fatal_count {

  def main(agrs: Array[String]) =
    {
      val conf = new SparkConf()

        .setAppName("ETLFatal")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("C:/Users/IBM_ADMIN/Documents/ETL/Production Issues/39061/ADS_sbox2_ETL_Logs_20160301/ContractTaskFactLoadIns_DSDirect.txt")
      val upper = lines.filter(line => line.contains("Error code:"))
      val col = upper.count()
      for(line <- upper) println(line)
      println("Total fatal count *************")
      println(col)
    }
     
}