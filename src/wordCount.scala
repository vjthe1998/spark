import org.apache.spark.SparkContext




object wordCount extends App{
  

  val sc=new SparkContext("local[*]","wc")
  val file=sc.textFile("C:\\Users\\Vipul\\OneDrive\\Desktop\\test.txt")
  val words=file.flatMap(_.split(" "))
  val lower=words.map(_.toLowerCase())
  val wordMap=lower.map(x=>(x,1))
  val reduce=wordMap.reduceByKey((a,b)=>a+b)
  val finals=reduce.sortBy(_._2,false)
  val results=finals.collect
  for(result<-results)
  {
    val key=result._1;
    val value=result._2;
    println(s"$key : $value")
  }
    
}