package com.westpack.preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object wordcount {
  
 def main(args:Array[String]){
   
   
   val conf=new SparkConf().setAppName("wordcount").setMaster("local")
    val spark=new SparkContext(conf)
    val file_rdd=spark.textFile("file:///E:/Karuna/Hadoop/Input/wordcount.txt")
    val wordcount=file_rdd.flatMap(line=>line.split(" "))
                          .map(w=>(w,1))
                          .reduceByKey(_+_)
    wordcount.collect.foreach(println)
    
    val maxword=file_rdd.flatMap(line=>line.split(" "))
                          .map(w=>(w,w.length)).reduce((a,b) => if (a._2 > b._2) a else b)
 println(maxword)
 println(file_rdd.flatMap(line=>line.split(" ")).map(w=>(w.length,w)).max)
 println("welcome")
 
getFibonacci1(10)


var list=List("karuna","solomon","raju","raju")
println(list.map(w=>(w.length,w)).max)
 
 }
 
 
 def getFibonacci1(index:Int){
   
   @annotation.tailrec
   def getNumbers(index:Int,prev:Int,current:Int){
     println(current)
     if(index<=0){
       current
     }else{
       getNumbers(index-1,prev=prev+current,current=prev)
    }
     
   }
   getNumbers(index,prev=1,current=0)
   
   
 }
 
 
 
  

 
 def getFibonacci(index: Int): Int = {
  @annotation.tailrec
  def getTailRec(index: Int, prev: Int, current: Int): Int = {
   // println("current index: %d".format(index))
    println("current value: %d".format(current))
  //  println("prev value: %d".format(prev))
    if (index <= 0) {
      current
    } else {
      getTailRec(index - 1, prev = prev + current, current = prev)
    }
  }

  getTailRec(index, prev = 1, current = 0)
}
  
}