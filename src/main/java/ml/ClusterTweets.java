package ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.List;

public class ClusterTweets {
    public static void main(String... args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // Set hadoop directory for windows
//        System.setProperty("hadoop.home.dir", args[0]);

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
        JavaRDD<String> texts = sqlContext.sql("SELECT text from tweetTable").javaRDD().map(t -> t.toString());
        // Cache the vectors RDD since it will be used for all the KMeans iterations.
        JavaRDD<Vector> vectors = texts.map(Utils.featurize).cache();
        vectors.count();  // Calls an action on the RDD to populate the vectors cache.

        // Cluster the data using KMeans
        int numClusters = 5;
        int numIterations = 20;
        KMeansModel model = KMeans.train(vectors.rdd(), numClusters, numIterations);
        // Persist the model so it can be loaded later
        model.save(sc.sc(), "tweetsClusterer");

        List<String> someTweets = texts.take(100);
        System.out.println("----Example tweets from the clusters----");
        for(int i = 0; i < numClusters; i++) {
            System.out.println("CLUSTER " + i);
            final int finalI = i;
            someTweets.forEach(t -> {
                try {
                    if(model.predict(Utils.featurize.call(t)) == finalI) {
                        System.out.println(t);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
