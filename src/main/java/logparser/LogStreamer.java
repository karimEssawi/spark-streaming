package logparser;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LogStreamer {
    private final JavaStreamingContext context;

    public LogStreamer(SparkConf conf) {
        // Create a new Context with our config with 10 seconds batch size
        context = new JavaStreamingContext(new JavaSparkContext(conf), Durations.seconds(10));
    }

    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
    private static Function2<List<Long>, Optional<Long>, Optional<Long>> COMPUTE_RUNNING_SUM = (nums, current) -> {
        long sum = current.or(0l);
        for(long n : nums) {
            sum += n;
        }
        return Optional.of(sum);
    };

    // These static variables stores the running content size values.
    private static final AtomicLong runningCount = new AtomicLong(0);
    private static final AtomicLong runningSum = new AtomicLong(0);
    private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);


    public static Tuple4<Long, Long, Long, Long> contentSizeStats(JavaRDD<ApacheAccessLog> accessLogRDD) {
        JavaRDD<Long> contentSizes = accessLogRDD.map(ApacheAccessLog::getContentSize);
        return new Tuple4<>(contentSizes.count(), contentSizes.reduce(SUM_REDUCER),
                            contentSizes.min(Comparator.naturalOrder()),
                            contentSizes.max(Comparator.naturalOrder()));
    }

    public static JavaPairRDD<Integer, Long> responseCodeCount(JavaRDD<ApacheAccessLog> accessLogRDD) {
        return accessLogRDD
                .mapToPair(l -> new Tuple2<>(l.getResponseCode(), 1L))
                .reduceByKey(SUM_REDUCER);
    }

    public static JavaPairRDD<String, Long> ipAddressCount(JavaRDD<ApacheAccessLog> accessLogRDD) {
        return accessLogRDD
                .mapToPair(l -> new Tuple2<>(l.getIpAddress(), 1L))
                .reduceByKey(SUM_REDUCER);
    }

    public static JavaPairRDD<String, Long> endPointCount(JavaRDD<ApacheAccessLog> accessLogRDD) {
        return accessLogRDD
                .mapToPair(l -> new Tuple2<>(l.getEndpoint(), 1L))
                .reduceByKey(SUM_REDUCER);
    }

    public LogStreamer run() {
        context.checkpoint("checkpoint");

        JavaReceiverInputDStream<String> logDataStream = context.socketTextStream("localhost", 9999);

        JavaDStream<ApacheAccessLog> accessLogStream = logDataStream.map(ApacheAccessLog::parseFromLogLine);

        // Calculate statistics based on the content size, and update the static variables to track this.
        accessLogStream.foreachRDD( rdd -> {
            if(rdd.count() > 0) {
                Tuple4<Long, Long, Long, Long> contentSizeStats = contentSizeStats(rdd);
                runningCount.getAndAdd(contentSizeStats._1());
                runningSum.getAndAdd(contentSizeStats._2());
                runningMin.set(Math.min(runningMin.get(), contentSizeStats._3()));
                runningMax.set(Math.max(runningMax.get(), contentSizeStats._3()));

                System.out.print("Content Size Avg: " + runningSum.get() / runningCount.get());
                System.out.print(", Min: " + runningMin.get());
                System.out.println(", Max: " + runningMax.get());
            }
            return null;
        });

        // Compute Response Code to Count.
        // Notice the use of transformToPair to produce the a DStream of
        // response code counts, and then updateStateByKey to accumulate
        // the response code counts for all of time.
        JavaPairDStream<Integer, Long> responseCount = accessLogStream
                                                            .transformToPair(LogStreamer::responseCodeCount)
                                                            .updateStateByKey(COMPUTE_RUNNING_SUM);
        responseCount.foreachRDD(rdd -> {
            System.out.println("Response code counts: " + rdd.take(100));
            return null;
        });

        // A DStream of ipAddresses accessed > 10 times.
        JavaDStream<String> ipAddressesStream = accessLogStream
                                                    .transformToPair(LogStreamer::ipAddressCount)
                                                    .updateStateByKey(COMPUTE_RUNNING_SUM)
                                                    .filter(t -> t._2() > 10)
                                                    .map(Tuple2::_1);
        ipAddressesStream.foreachRDD(rdd -> {
            System.out.println("IP Addresses > 10 times: " + rdd.take(100));
            return null;
        });

        // A DStream of endpoint to count.
        JavaPairDStream<String, Long> endpointCount = accessLogStream
                                                        .transformToPair(LogStreamer::endPointCount)
                                                        .updateStateByKey(COMPUTE_RUNNING_SUM);
        endpointCount.foreachRDD(rdd -> {
            List<Tuple2<String, Long>> topEndpoints = rdd.top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
            System.out.printf("Top Endpoint: " + topEndpoints);
            return null;
        });

        context.start();
        return this;
    }

    public void awaitTermination() {
        context.awaitTermination();
    }

    private static class ValueComparator<T, V> implements Comparator<Tuple2<T, V>> {
        Comparator<V> comparator;
        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<T, V> a, Tuple2<T, V> b) {
            return comparator.compare(a._2(), b._2());
        }
    }

    public static void main(String[] args) {
        // Set hadoop directory for windows
        System.setProperty("hadoop.home.dir", args[0]);

        SparkConf conf = new SparkConf().setAppName("Log_Streamer").setMaster("local[2]");
        LogStreamer logAnalyzerStreaming = new LogStreamer(conf).run();
        logAnalyzerStreaming.awaitTermination();
    }
}
