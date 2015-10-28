package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

public class TrainClassifier {
    public static void main(String... args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // Create a DataFrame out of tweetInput and cache it
        String tweetInput = "tweets/tweets*/part-*";
        DataFrame tweetTable = sqlContext.read().json(tweetInput).cache();
        tweetTable.registerTempTable("tweetTable");

        System.out.println("------Tweet table Schema-------");
        tweetTable.printSchema();

        System.out.println("------Sample Lang, Name, text-------");
        sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 100").collectAsList().forEach(System.out::println);

        System.out.println("------Total count by languages Lang, count(*)-------");
        sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 50").collectAsList().forEach(System.out::println);

        System.out.println("------Training the model and persist it-------");

        JavaRDD<String> texts = sqlContext.sql("SELECT text from tweetTable").javaRDD().map(Row::toString);
        // Cache the vectors RDD since it will be used for all the KMeans iterations.
        Utils utils = new Utils();
//        JavaRDD<Vector> vectors = texts.map(t -> utils.featurize(t)).cache();
//        vectors.count();  // Calls an action on the RDD to populate the vectors cache.
    }
}
