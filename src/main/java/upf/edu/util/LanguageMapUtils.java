package upf.edu.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
        // (language code, english language name)
        return lines
                .map(t -> t.split("\t")) // Separating by tab
                .mapToPair(p -> new Tuple2<>(p[1], p[2]))
                .distinct(); // Remove duplicates
    }
}
