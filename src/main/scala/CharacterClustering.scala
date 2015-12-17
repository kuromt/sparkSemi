import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, Word2Vec}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline

object CharacterClustering {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("CharacterClustering"))
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    
    //
    val data = sc.textFile("/data/hanze_and_gretel.txt").toDF("text").cache
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("removedSW")
    val word2Vec = new Word2Vec().setInputCol("removedSW").setOutputCol("word2vector").setVectorSize(10).setMinCount(0)

    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, word2Vec))

    val model = pipeline.fit(data)
    val result  = model.transform(data)
    val kmeans = new KMeans().setFeaturesCol("word2vector").setPredictionCol("label").setK(3)
    val kmeansModel = kmeans.fit(result)
    val labeled = kmeansModel.transform(result)
    labeled.show

    sc.stop()
  }
}