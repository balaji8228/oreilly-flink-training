package flink.tryyourself;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use Case and Program Requirements:
        // I will receive the "input data" below twice every 5 seconds,
        // I want to know what are the words (lowercased) that are neither within stopwords nor empty string,
        // and how many times they appear in the past 5 sec

        // input data:
//        I am his play you read, watch, write, listen, her she it short long int read double
//        float double you read, watch, write, listen, her she it short listen, her she it short
//        long int am his play you read float double you read

        // Start Websocket:
        // $ nc -lk 9999

        // API References:
        // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/
        // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html

//        DataStream<Tuple2<String, Integer>> dataStream = env
//                // listen to websocket at port 9999 of localhost
//                .socketTextStream("localhost", 9999)
//                // tokenize sentenses
//                // filter out invalid data
//                // key by word
//                // set tumbling windows for every 5 sec
//                  // sum to count
//                ...;
//
//        dataStream.print();

        // expected output
//        (watch,4)
//        (write,4)
//        (read,10)
//        (play,4)
//        (listen,6)
//        (double,6)
//        (float,4)
//        (int,4)
//        (long,4)
//        (short,6)

        // execute the Flink program
        env.execute("Window WordCount");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.toLowerCase().split("\\W+")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    final static Set<String> STOP_WORDS = new HashSet<>(
            Arrays.asList(
                    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
                    "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "it", "its", "itself", "they", "them",
                    "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those",
                    "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
                    "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
                    "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
                    "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on",
                    "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why",
                    "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
                    "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
                    "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "didn", "hadn", "ma", "mightn", "needn"
            )
    );

}
