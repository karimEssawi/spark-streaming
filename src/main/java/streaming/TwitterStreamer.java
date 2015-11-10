package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

public class TwitterStreamer {
    private final JavaStreamingContext context;

    public TwitterStreamer(SparkConf conf) {
        // Create a new Context with our config and a 1 second batch size
        context = new JavaStreamingContext(new JavaSparkContext(conf), Durations.seconds(30));
    }

    // These must be public and static as they have to be serialized, so private method
    // references will blow up with java.io.NotSerializableException
    public static Function2<Integer,Integer,Integer> add = (a, b) -> a + b;
    public static Function2<Integer,Integer,Integer> subtract = (a, b) -> a - b;

    private Function<JavaPairRDD<String, Integer>,Void> printTopN(int n) {
        return (JavaPairRDD<String, Integer> rdd) -> {
            rdd.take(n).forEach(System.out::println);
            return null;
        };
    }

    public TwitterStreamer run() {
        context.checkpoint("checkpoint");

        // Create a stream
        String[] filters = new String[] {"bmw", "volkswagen", "audi"};
        JavaDStream<Status> stream = TwitterUtils.createStream(context, filters);

        // Convert it to a stream of hashtags
        JavaDStream<String> hashTags = stream.flatMap(t -> Arrays.asList(t.getText().split(" "))).filter(h -> h.startsWith("#"));

        // Count the hashtags over a 5 minute window
        // Map each tag to a (tag, 1) key-value pair
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(h -> new Tuple2<>(h, 1));
        // Then reduce by adding the counts
        JavaPairDStream<String, Integer> countsWindow = tuples.reduceByKeyAndWindow(add, // Add new elements
                                                                              subtract, // subtract leaving elements
                                                                              Durations.minutes(5), // Window size
                                                                              Durations.seconds(30));   // Slide along every 30 seconds

        // Find the top 10 hashtags based on their counts
        countsWindow.mapToPair(Tuple2::swap)
                .transformToPair(c -> c.sortByKey(false))
                .mapToPair(Tuple2::swap)
                .foreachRDD(printTopN(10));

        context.start();
        return this;
    }

    public void awaitTermination() {
        context.awaitTermination();
    }
}
