package io;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by hirokinaganuma on 2016/09/29.
 *
 * うまくいってないけど一旦保留
 *
 */
public class JsonSample1 {


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tweet implements java.io.Serializable{
        private final String createdAt;
        private final long id;
        private final String text;
        private final Boolean retweeted;

        @JsonCreator
        public Tweet(@JsonProperty("created_at") String createdAt,
                     @JsonProperty("id") long id,
                     @JsonProperty("retweeted") Boolean retweeted,
                     @JsonProperty("text") String text) {
            this.createdAt = createdAt;
            this.id = id;
            this.text = text;
            this.retweeted = retweeted;
        }

        public String getCreatedAt() {
            return createdAt;
        }

        public long getId() {
            return id;
        }

        public String getText() {
            return text;
        }
    }

    //JavaでのJSONのロード
    public static class ParseJson implements FlatMapFunction<Iterator<String>, Tweet> {
        public Iterable<Tweet> call(Iterator<String> lines) throws Exception {
            ArrayList<Tweet> tweet = new ArrayList<Tweet>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    tweet.add(mapper.readValue(line, Tweet.class));
                } catch (Exception e) {
                    // Skip invalid input
                }
            }
            return tweet;
        }
    }

    public static class IsRetweet implements Function<Tweet, Boolean> {
        public Boolean call(Tweet tweet) {
            return tweet.retweeted;
        }
    }


    public static class WriteJson implements FlatMapFunction<Iterator<Tweet>, String> {
        public Iterable<String> call(Iterator<Tweet> tweets) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (tweets.hasNext()) {
                Tweet tweet = tweets.next();
                text.add(mapper.writeValueAsString(tweet));
            }
            return text;
        }
    }

    public static void main(String[] args) throws Exception {

        String master ;
        String inputFile = "bin/data/*.json";//まとめて読み込むことも可能
        String outputPath = "result/output02";
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(inputFile);

        //パーティションを別の型のパーティションに変換する
        JavaRDD<Tweet> result = input.mapPartitions(new ParseJson()).filter(new IsRetweet());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile(outputPath);
    }
}