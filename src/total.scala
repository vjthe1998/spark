import org.apache.spark.SparkContext


object total extends App {
  val sc=new SparkContext("local[*]","tot")
  val file=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\customerorders-201008-180523.csv")
  val filter=file.map( x=>(x.split(",")(0), x.split(",")(2).toFloat ))
  val reduce=filter.reduceByKey((a,b)=>a+b)
  val sort=reduce.sortBy(x=>x._2,false)
  val results=sort.collect
  for(result<-results)
  {
    val key=result._1;
    val value=result._2;
    println(s"$key : $value")
  }
}