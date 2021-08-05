package day0416.stream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyMultiDataSourceTest {
    public static void main(String[] args) throws Exception {
        //创建接口
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建数据源
        DataStreamSource<Integer> source = sEnv.addSource(new MyMultiDataSource()).setParallelism(3);

        source.map(new RichMapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return "数据是"+value;
            }
        }).print();
        sEnv.execute("MyMultiDataSourceTest");



    }
}
