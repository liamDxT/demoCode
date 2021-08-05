package day0419.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.math.Integral;

public class FlinkDataStreamDemo1 {
    public static void main(String[] args) throws Exception {
        // filter过滤
        // 创建访问接口：StreamExecutionEnvironment
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建数据源
        DataStreamSource<Integer> source = sEnv.addSource(new MySingleDataSource());

        source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value%2 == 0?true:false;
            }
        }).map(new RichMapFunction<Integer,String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return "过滤后的数据为："+integer;
            }
        }).print();
        sEnv.execute("FlinkDataStreamDemo1");

    }

}
