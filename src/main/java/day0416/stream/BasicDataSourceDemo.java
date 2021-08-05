package day0416.stream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class BasicDataSourceDemo {
    public static void main(String[] args) throws Exception {
        //创建一个数据集合
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);

        //创建访问接口 StreamExecutionEnvironment
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建数据源
        DataStreamSource<Integer> source = sEnv.fromCollection(list);

        //执行计算: 每个元素+5
        source.map(new RichMapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer value) throws Exception {
                return value+5;
            }
        }).print().setParallelism(1);
        sEnv.execute("BasicDataSourceDemo");


    }
}
