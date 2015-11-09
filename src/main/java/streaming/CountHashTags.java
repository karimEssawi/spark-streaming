package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

public class CountHashTags {

    public static void main (String... args) {
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
        jssc.checkpoint("checkpoint");
        // Set hadoop directory for windows
//        System.setProperty("hadoop.home.dir", args[0]);
        String[] filters = new String[] {"autotrader", "auto trader", "autotraderlife"};

        JavaDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
        JavaDStream<String> hashTags = stream.flatMap(t -> Arrays.asList(t.getText().split(" "))).filter(h -> h.startsWith("#"));

        // Count the hashtags over a 5 minute window
        // Map each tag to a (tag, 1) key-value pair
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(h -> new Tuple2<>(h, 1));
        // Then reduce by adding the counts
        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow((a, b) -> a + b, // Add new counts to sliding window
                                                                              (a, b) -> a - b, // subtract counts that falls out of the window
                                                                              Durations.minutes(5), // Window size
                                                                              Durations.seconds(30));   // Slide along every 30 seconds

        // Find the top 10 hashtags based on their counts
        counts.mapToPair(Tuple2::swap).transformToPair(c -> c.sortByKey(false)).foreachRDD(c -> {
            String out = "\nTop 10 hashtags:\n";
            for (Tuple2<Integer, String> t : c.take(10)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);
            return null;
        });

//        hashTags.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
