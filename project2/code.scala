// Databricks notebook source
// Step 1: Download CSV file to DBFS
import sys.process._ 
import sys.process.ProcessLogger

"wget -P /FileStore https://github.com/vincentarelbundock/Rdatasets/blob/master/csv/datasets/AirPassengers.csv" !!

// COMMAND ----------

// Step 2: Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD
def mapName(x: Float): String =  x match {
    case 0.0 => "Q1"
    case 0.25 => "Q2"
    case 0.50 => "Q3"
    case 0.75 => "Q4"
    case _ => "NaN"
  }

val csv = sc.textFile("/FileStore/tables/UKgas.csv")
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
val header = headerAndRows.first
val data = headerAndRows.filter(_(0) != header(0))
val population = data.map(row => (mapName((row(1).toFloat)%1),row(2).toDouble)).sortBy(_._1).cache()
population.collect().foreach(println)

// COMMAND ----------

// Step 3: Compute the mean mpg and variance 
import org.apache.spark.sql.functions.{avg => favg, stddev_samp => fstddev_samp, pow => fpow}

case class Stats(quater: String, count: Int, sum: Double, sumSquare : Double)
val square = (x : Double) => math.pow(x, 2)
val doStat = (sample: RDD[(String, Double)]) => {
  sample.groupByKey().sortByKey()
  .map(x => new Stats(x._1, x._2.count(_=>true), x._2.reduce(_+_), x._2.map(square).reduce(_+_)))
}

val popStat = doStat(population)
.map(x => (x.quater, x.sum/x.count, math.sqrt(x.sumSquare/x.count - square(x.sum/x.count))))
.toDF("quater", "mean", "std")
popStat.show()

//   collect().foreach(x => println(x.quater + "\t" + x.sum/x.count + "\t" + math.sqrt(x.sumSquare/x.count - square(x.sum/x.count))))

// COMMAND ----------

// Step 4: Create the sample for bootstrapping
val sample  = population.sample(false, 0.25)
sample.toDF.show()

// COMMAND ----------

// Step 5: ResampledData 1000 times
import collection.mutable.Map
import org.apache.spark.sql.functions.{avg => favg, stddev_samp => fstddev_samp, pow => fpow}

var total: Map[String, Map[String, Double]] = Map()

// initialize total map
sample.toDF("key", "value").select("key").distinct().collect().foreach(quater => {
  total(quater(0).toString) = Map("mean" -> 0.0, "std" -> 0.0)
})

// do iteration
val numberIteration  = 100
for(i <- 1 to numberIteration) {
  val s = sample.sample(true, 1)
  
  val method1 = s.toDF("key", "value").groupBy("key")
  // In case of s have only 1 record, fstddev_samp => NaN (b/c it is devided by 0) => fill with 0 b/c there is no deviation
    .agg( favg("value").alias("mean"),  fstddev_samp("value").alias("std")).orderBy("key").na.fill(0)

  /*val method2 = doStat(s).collect()
  .foreach(x => println(x.quater + "\t" + x.sum/x.count + "\t" + math.sqrt(x.sumSquare/x.count - square(x.sum/x.count))))*/
  
  method1.rdd.collect().foreach(arr => {
    total(arr(0).toString)("mean") += arr(1).toString.toDouble
    total(arr(0).toString)("std") += arr(2).toString.toDouble
  })
}

// calculate avarage
for((k, v) <- total) {
  v("mean") /= numberIteration
  v("std") /= numberIteration
}

// print results
var a : List[(String,Double, Double)] = List()
for((k, v) <- total) {
  a = a:+((k, v("mean"), v("std")))
//   println(k + "\t" + "%.3f".format(v("mean")) + "\t" + "%.3f".format(v("std")))
} 
val b = a.toDF("quater", "mean", "std").orderBy("quater")
b.show()
// display(b)