package com.westpack.preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
 import org.apache.spark.rdd.DoubleRDDFunctions
//import java.util.Collection
import java.io.File
import scala.io.Source._



case class person(name:String,ID:Int,Year:Int)
case class Employee(id: Int, name: String, age: Int)
case class Department(dept_no:String,dept_name:String)
 


object merge {
  
  
  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""
  
  
   private val NPARAMS = 2

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }
  
  
    def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
     .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }
    
    
  def main(args:Array[String]){


//read from local file system
 println("Performing local word count")
 val fileContents = readFile("E:/Karuna/Hadoop/Input.txt")
 val localWordCount = runLocalWordCount(fileContents)
println(localWordCount)
 
    
    val conf = new SparkConf().setAppName("merge").setMaster("local")
val sc= new SparkContext(conf)
val sqlContext=new SQLContext(sc)
    



println("Reading file from DFS and running Word Count")
    val readFileRDD = sc.textFile("file:///E:/Karuna/Hadoop/Input/Input.txt")

    println(readFileRDD.flatMap(x=>x.split(",")).map(x=>x).collect.length)//.mean//.collect.foreach(println)
    readFileRDD.collect.foreach(println)
    
    /*
    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum


  if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count $localWordCount and " +
        s"DFS Word Count $dfsWordCount agree.")
    } else {
      println(s"Failure! Local Word Count $localWordCount " +
        s"and DFS Word Count $dfsWordCount disagree.")
    }
    
    */
    
//def addOne=(i:Int)=> i+2
//import org.apache.spark.sql.functions.udf
//val addition=udf(addOne)


//val input_rdd = sc.textFile("file:///E:/Karuna/Hadoop/Input/Input.txt").map(x=>x.toInt)
//val add=input_rdd.map(x=>addOne(x))
//add.collect.foreach(println)


//val input_rdd = sc.textFile("file:///E:/Karuna/Hadoop/Input/Input.txt").map(x=>x.toInt)
//println(input_rdd.mean)

//println("Sum:"+ input_rdd.sum+"  Count:"+input_rdd.collect.length)
    
    
//val departments = sc.textFile("file:///E:/Karuna/Hadoop/departments/part-m-00000.txt")//.trim()//.map(_.split("\n"))
//println("partion size" + departments.partitions.length)
//departments.saveAsTextFile("file:///E:/Karuna/Hadoop/departments/splits")

//val employees1=sc.parallelize(Seq((1,"karuna"),(2,"raju"),(1,"karuna")))
//val groupbykey=employees1.groupByKey()
//groupbykey.collect.foreach(println)


//val groupby=employees1.groupBy(_._1)
//groupby.collect.foreach(println)


//val filter=departments.filter(!_.isEmpty)
//import sqlContext.implicits._
//val depSplit=filter.map(l=>l.trim().split(",")).map(d=>Department(d(0).toString,d(1).toString))
//println(depSplit.collect.length)
//depSplit.collect.foreach(println)

//for(i <- 0.until(bindschema.count)){println(item(i))}
//.map(_.split(",")).map( d => Department(d(0),d(1)))


//Map schema to values
val employees=sc.parallelize(Seq((1,"karuna"),(2,"raju")))
val bind=employees.map(l=>Department(l._1.toString,l._2))
//val item=employees.map(item=>item._2)
//bind.collect.foreach(println)
//item.collect.foreach(println)


//List Operations
var list=List("karuna","solomon","raju","raju")
println(list.map(_.length))
var largWordLength=list.map(_.length).max
println(list.map(_.length).max)
println(list.filter(_.length==largWordLength))
println(list(list.length-1))
println(list.head+"   "+list.tail)
println(list.distinct)
for(i <-0.until(list.length))
{println(list(i))}

// Create two String lists.
val positions1 = List("top", "bottom")
val positions2 = List("left", "right")
//println(positions1)
//println(positions2)

// Combine the two lists with an operator.
val positions3 = positions1 ++ positions2
//println(positions3)


//RDD operations
val emp=sc.textFile("file:///E:/Karuna/Employee.txt")
val length=emp.map(_.split(" ").length).max
val splitdata=emp.flatMap(_.split(" "))
//println(splitdata)
//println(splitdata.filter{_.length==length})




val text = sc.parallelize(Seq("hi this is my stringsfdsgdf, lets word count it karunakar rajuwe end"))

val length1=text.flatMap(_.split(" ")).map(w=>w.length).max
val split=text.flatMap(_.split(" "))
val result = split.filter{_.size == length1}
//result.collect.foreach(println)
//val shiveContext=new HiveContext(sc)

//import org.apache.spark.sql.DataFrame
//import sqlContext.implicits._

//
//val df=spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").load("file:///E:/Karuna/Employee.txt")
//df.show()
/*
val empl=sc.textFile("file:///E:/Karuna/Employee.txt")
.map(_.split(","))
.map(e⇒ Employee(e(0).trim.toInt,e(1), e(2).trim.toInt))
.toDF()
empl.registerTempTable("employee")
val allrecords = sqlContext.sql("SELECT * FROM employee")
allrecords.printSchema()
val agefilter = sqlContext.sql("SELeCT * FROM employee WHERE age>=20 AND age <= 35")
//agefilter.map(t=>"ID: "+t(0)).collect().foreach(println)
*/
//allrecords.show()

/* pattern Matching 
val p1= Seq(person("karuna",33,1985),person("karuna1",33,1985),person("karuna3",33,1985))
p1.foreach(println)

def getName( x: String) : String =
  x match{
    case "karuna"=>"Karuna"
    case "karuna1"=>"karuna1"
    case "karuna2"=>"karuna2"
    case _=>"None"
    
  }
  

var return1=getName("hi")
println(return1)
*/
/* RDD Sorting
 val data=Array(1,2,3,4)
val rdd=sc.parallelize(data)
rdd.collect.foreach(println)
val sorted=rdd.sortBy(x=>x,false)
sorted.foreach(println)
*/
/* Load text file and read RDD
val distFile = sc.textFile("file:///C:/Hadoop/input.txt")
val split=distFile.flatMap(line=>line.split(" ")).map(word=>(word.trim,1)).reduceByKey(_+_)
split.foreach(println)
*/

//Map side join

// Fact table
/*
val flights = sc.parallelize(List(
  ("SEA", "JFK", "DL", "418",  "7:00"),
  ("SFO", "LAX", "AA", "1250", "7:05"),
  ("SFO", "JFK", "VX", "12",   "7:05"),
  ("JFK", "LAX", "DL", "424",  "7:10"),
  ("LAX", "SEA", "DL", "5737", "7:10")))  
   
// Dimension table
val airports = sc.parallelize(List(
  ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
  ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
  ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
  ("SFO", "San Francisco International Airport", "San Francisco", "CA")))
   
// Dimension table
val airlines = sc.parallelize(List(
  ("AA", "American Airlines"), 
  ("DL", "Delta Airlines"), 
  ("VX", "Virgin America")))

val airportMap=sc.broadcast(airports.map{case(a,b,c,d)=>(a,c)}.collectAsMap)
val airlineMap=sc.broadcast(airlines.collectAsMap)
val flightsMap= flights.map{case(a,b,c,d,e) =>
                       (airportMap.value.get(a).get,
                       airportMap.value.get(b).get,
                       airlineMap.value.get(c).get,
                       d,e)}.collect
flightsMap.foreach(println)

*/
//val hc=new org.apache.spark.sql.hive.HiveContext(spark)
//val df=hc.read.format("csv").load("file:///C:/Hadoop/input.csv")
//df.show

   
    
  }
}