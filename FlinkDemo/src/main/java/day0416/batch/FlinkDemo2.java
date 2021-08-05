package day0416.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Filter与Distinct
 * 过滤与去重
 */
public class FlinkDemo2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source =env.fromElements("I love Beijing","I love China","Beijing is the capital of China");

        DataSet<String> flatResult = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //分词
                String[] words = value.split(" ");
                for (String w:words) {
                    out.collect(w);
                }
            }
        });

        //去掉重复的单词
        flatResult.distinct().print();
        System.out.println("--------------------------");

        //选择长度大于5的单词
        flatResult.filter(new FilterFunction<String>() {
            //过滤逻辑
            @Override
            public boolean filter(String value) throws Exception {
                int length = value.length();
                return length>=5?true:false;
            }
        }).print();









    }
}
