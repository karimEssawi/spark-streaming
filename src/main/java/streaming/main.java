package streaming;

import org.apache.spark.SparkConf;

public class main {
    public static void main (String... args) throws Exception {
        // Set hadoop directory for windows
        System.setProperty("hadoop.home.dir", args[0]);

        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        TwitterStreamer twitterStreamer = new TwitterStreamer(conf).run();
        twitterStreamer.awaitTermination();
    }
}
