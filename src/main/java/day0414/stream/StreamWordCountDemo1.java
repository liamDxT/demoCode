package day0414.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCountDemo1 {
    public static void main(String[] args) throws Exception {
        //创建接口:
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //从netCat上接受数据
       DataStreamSource<String> source= sEnv.socketTextStream("bigdata111",1234);

        DataStream<Tuple2<String,Integer>> result = source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
           @Override
           public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
               String[] words = s.split(" ");
               for (String w:words) {
                   collector.collect(new Tuple2<String,Integer>(w,1));
               }

           }
       }).keyBy(0).sum(1);

        result.print().setParallelism(2);
        sEnv.execute("StreamWordCountDemo1");


    }

}
