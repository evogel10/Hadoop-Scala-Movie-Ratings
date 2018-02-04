/* movie_rating.scala*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object MovieRating {
	def main(ars: Array[String]) {
		val conf = new SparkConf().setAppName("Movie Rating")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR");

		var uItemRDD = sc.textFile("hdfs://localhost:9000/movie-ratings-data/u.item")

		val uItemTupleRDD = uItemRDD.map(line => (line.split("\\|")(0), (line.split("\\|")(0), line.split("\\|")(1), line.split("\\|")(2), line.split("\\|")(4))))

		var uDataRDD = sc.textFile("hdfs://localhost:9000/movie-ratings-data/u.data")

		val uDataTupleRDD = uDataRDD.map(line => (line.split("\t")(1), line.split("\t")(2).toFloat))

		val groupedUDataTupleRDD = uDataTupleRDD.groupByKey().mapValues(_.toList)

		def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = { num.toDouble( ts.sum ) / ts.size}

		val averageGroupedUDataTupleRDD = groupedUDataTupleRDD.map(rec => (rec._1, average(rec._2)))

		val joinedRDD = uItemTupleRDD.join(averageGroupedUDataTupleRDD)

		val finalResultRDD= joinedRDD.map(rec => rec._2._1._1 + "," + rec._2._1._2 + "," + rec._2._1._3 + "," + rec._2._1._4 + "," + rec._2._2)

		finalResultRDD.saveAsTextFile ("hdfs://localhost:9000/output_ratings")

		sc.stop()

	}
}