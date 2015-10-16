import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import java.io.File;

/**
 * Collect at least the specified number of tweets into json text files.
 */
public class collectTweets {
    public static void main (String... args) {
        Integer intervalInSeconds = 1000;
        final long[] numTweetsCollected = {0l};
        Gson gson = new Gson();

        // Create tweets directory
        File outputDir = new File("tweets");
        if(!outputDir.exists()) {
            outputDir.mkdir();
        }

        // Initialise Spark Streaming context
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(intervalInSeconds));
        jssc.checkpoint("checkpoint");
        System.setProperty("hadoop.home.dir", args[0]);

        // Create twitter stream and map incoming tweets to gson
        JavaDStream<String> tweetStream = TwitterUtils.createStream(jssc).map(gson::toJson);
        // Save to directory
        tweetStream.foreachRDD((rdd, time) -> {
            Long count = rdd.count();
            if(count > 0) {
                JavaRDD outputRDD = rdd.repartition(10); //number of output files written for each interval
                outputRDD.saveAsTextFile(outputDir + "/tweets_" + time.milliseconds());
                numTweetsCollected[0] += count;
                if (numTweetsCollected[0] > 5000) {
                    System.exit(0);
                }
            }
            return null; // foreachRDD accepts a Function<JavaRDD<...>, Void>
        });

        jssc.start();
        jssc.awaitTermination();

    }
}
