package ml;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

import java.util.Arrays;

public class Utils {
    private static HashingTF tf = new HashingTF(1000);
    public static Function<String, Vector> featurize = (s) -> tf.transform(Arrays.asList(s.split(" ")));
}
