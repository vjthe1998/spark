

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext



object accumalators extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","bro")
  val source=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\week10\\samplefile-201014-183159.txt")
  val myac=sc.longAccumulator("blank link accumulator")
  source.foreach(x=>if(x==" ")myac.add(1))
  myac.value
}