package day0414.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountDemo1 {
    public static void main(String[] args) throws Exception {

        //访问接口ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataSet<String> source = env.fromElements("I love Beijing","I love China","Beijing is the capital of China");
        source.print();
        // 数据源  返回值类型
        DataSet<Tuple2<String,Integer>> result=

                source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String w:words
                     ) {
                    collector.collect(new Tuple2<String, Integer>(w,1));
                }
            }
        }).groupBy(0).sum(1);
        result.print();
        //提交执行
      // env.execute("WordCountDemo1");
    }
}
