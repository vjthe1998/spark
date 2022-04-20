import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object money extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","money")
  val source=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\week10\\bigdatacampaigndata-201014-183159.csv")
  val filter=source.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  val word=filter.flatMapValues(x=>x.split(" "))
  val order=word.map(x=>(x._2.toLowerCase,x._1))
  val reduce=order.reduceByKey((x,y)=>x+y)
  val sort=reduce.sortBy(x=>x._2,false)
  sort.take(20).foreach(println)
  
}