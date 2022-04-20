import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source



object Broadcast extends App {
  
def loadBoaringWords()={
  var boaringWords:Set[String]=Set()
 val lines= Source.fromFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\week10\\boringwords-201014-183159.txt").getLines()
  for(line<-lines)
    boaringWords += line
    boaringWords
}
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","bro")
  val source=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\week10\\bigdatacampaigndata-201014-183159.csv")
  var nameSet=sc.broadcast(loadBoaringWords)
  val filter=source.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  val word=filter.flatMapValues(x=>x.split(" "))
  val order=word.map(x=>(x._2.toLowerCase,x._1))
  val xyz=order.filter(x=>(!nameSet.value(x._1)))
  val reduce=xyz.reduceByKey((x,y)=>x+y)
  val sort=reduce.sortBy(x=>x._2,false)
  sort.take(20).foreach(println)
  
}