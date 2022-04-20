import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source


object scala10 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","money")
 val lists=List("ERROR: Thu Jun 04 10:37:51 BST 2015",
 "WARN: Sun Nov 06 10:37:51 GMT 2016",
 "WARN: Mon Aug 29 10:37:51 BST 2016",
 "ERROR: Thu Dec 10 10:37:51 GMT 2015",
 "ERROR: Fri Dec 26 10:37:51 GMT 2014")
 val rdd=sc.parallelize(lists);
  val rdd1=rdd.map(x=>(x.split(":")(0),1))
  val rdd2=rdd1.reduceByKey((x,y)=>x+y)
  rdd2.collect.foreach(println)
  

}