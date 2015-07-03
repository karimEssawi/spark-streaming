import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class Tutorial {

    public static void main (String... args) {
        SparkConf conf = new SparkConf().setAppName("Spark_Streaming_Twitter").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(2000));

        String[] filters = new String[] {"England"};

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);
        stream.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
