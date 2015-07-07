import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

public class Tutorial {

    public static void main (String... args) {
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));

        String[] filters = new String[] {"egypt"};

        JavaDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
        JavaDStream<String> hashTags = stream.flatMap(t -> Arrays.asList(t.getText().split(" "))).filter(h -> h.startsWith("#"));

        // Count the hashtags over a 5 minute window
        // Map each tag to a (tag, 1) key-value pair
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(h -> new Tuple2(h, 1));
        // Then reduce by adding the counts
        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow((a, b) -> a + b, (a, b) -> a - b, new Duration(60 * 5 * 1000), new Duration(1 * 1000));

        counts.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
