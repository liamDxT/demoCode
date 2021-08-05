package day0414.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountDemo2 {
    public static void main(String[] args) throws Exception {
        String inputDir = "D:\\temp\\input";
        String outPutDir = "D:\\temp\\output";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source= env.readTextFile(inputDir);

        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {


            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" " );
                for (String w:words
                     ) {
                    collector.collect(new Tuple2<>(w,1));
                }
            }
        }).groupBy(0).sum(1).writeAsText(outPutDir).setParallelism(1);

        env.execute("WordCountDemo2");


    }
}
