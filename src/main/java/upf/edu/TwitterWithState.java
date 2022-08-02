package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.List;

public class TwitterWithState {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream
                .transformToPair(rdd -> rdd
                        .filter(t -> t.getLang().equals(language))
                        .mapToPair(tweet -> new Tuple2<>(tweet.getUser().getScreenName(), 1))
                        .reduceByKey(Integer::sum) // We reduce to get # of tweets per user
                );

        // Function2 to apply in updateStateByKey
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> reduceFunc = (values, state) -> {
            Integer newValue = state.orElse(0); // If element does not exist we start from 0
            newValue += values.stream().mapToInt(Integer::intValue).sum(); // We add all values
            return Optional.of(newValue);
        };

        // transform to a stream of <userTotal, userName> and get the first 20
        final JavaPairDStream<Integer, String> tweetsCountPerUser = tweetPerUser
                .updateStateByKey(reduceFunc)
                .transformToPair(rdd -> rdd // Transforming to sort
                        .mapToPair(Tuple2::swap)
                        .sortByKey(false)
                );

        // We print the first 20
        tweetsCountPerUser.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }

}
