package ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
public class CollectTweets {
    private Integer intervalInSeconds;
    private ObjectWriter writer;

    public CollectTweets(Integer intervalInSeconds) {
        this.intervalInSeconds = intervalInSeconds;
        this.writer = new ObjectMapper().writer();
    }

    public Integer getIntervalInSeconds() {
        return intervalInSeconds;
    }

    public void setIntervalInSeconds(Integer intervalInSeconds) {
        this.intervalInSeconds = intervalInSeconds;
    }

    public ObjectWriter getWriter() {
        return writer;
    }

    public void setWriter(ObjectWriter writer) {
        this.writer = writer;
    }

    public static void main (String... args) {
        CollectTweets collector = new CollectTweets(1000);

        final long[] numTweetsCollected = {0l};

        // Create tweets directory
        File outputDir = new File("tweets");
        if(!outputDir.exists()) {
            outputDir.mkdir();
        }

        // Initialise Spark Streaming context
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(collector.getIntervalInSeconds()));
        jssc.checkpoint("checkpoint");
        // Set hadoop directory for windows
//        System.setProperty("hadoop.home.dir", args[0]);

        // Create twitter stream and map incoming tweets to Json
        ObjectWriter writer = collector.getWriter();
        JavaDStream<String> tweetStream = TwitterUtils.createStream(jssc).map(writer::writeValueAsString);
        // Save to directory
        tweetStream.foreachRDD((rdd, time) -> {
            Long count = rdd.count();
            if(count > 0) {
                JavaRDD outputRDD = rdd.repartition(100); //number of output files written for each interval
                outputRDD.saveAsTextFile(outputDir + "/tweets_" + time.milliseconds());
                numTweetsCollected[0] += count; //bad, very bad!!!
                if (numTweetsCollected[0] > 50000) {
                    System.exit(0);
                }
            }
            return null; //foreachRDD accepts a Function<JavaRDD<...>, Void>
        });

        jssc.start();
        jssc.awaitTermination();

    }
}
