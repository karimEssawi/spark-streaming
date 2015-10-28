package ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;

public class Utils {
    private Integer numFeatures = 1000;
    private HashingTF tf = new HashingTF(numFeatures);

    public JavaRDD<Vector> featurize(JavaRDD s) {
        return tf.transform(s);
    }
}
