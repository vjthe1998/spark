import org.apache.spark.SparkContext


object movies extends App {
  val sc=new SparkContext("local[*]","movie")
  val file=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\moviedata-201008-180523.data")
//  val filter=file.map( x=>( x.split("	")(2), 1 ))
//  val reduce=filter.reduceByKey((a,b)=>a+b)
 val filter=file.map( x=> x.split("	")(2))
 val reduce=filter.countByValue

  reduce.foreach(println)
}