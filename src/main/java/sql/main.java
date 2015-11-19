package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

public class main {
    public static void main(String... args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Spark_Sql_App").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // Set hadoop directory for windows
        System.setProperty("hadoop.home.dir", args[0]);

        DataFrame tweets = sqlContext.read().json("datasets/tweets").cache();
        DataFrame users = sqlContext.read().json("datasets/users").cache();

        DataFrame joined = users.join(tweets, users.col("screenName").equalTo(tweets.col("user.screenName")), "left_outer");
        joined.registerTempTable("joinedTables");

        System.out.println("------Joined tables Schema-------");
        joined.printSchema();

        joined.collectAsList().forEach(System.out::println);
    }
}
