package day0416.batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 没有累加器计算结果
 * 设置并行度为4 即有4个进程同事执行任务
 * 计数结果为0---->1 1 1 1
 */

public class NoAccumuldatorDemo {
    public static void main(String[] args) throws Exception {
        //访问接口:ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //数据源
        DataSet<String> source = env.fromElements("Tom","Mary","Mike","Jone");
        
        //统计集合中的个数
        
        DataSet<Integer> result = source.map(new RichMapFunction<String, Integer>() {

            private  int total = 0;



            @Override
            public Integer map(String s) throws Exception {
                //计数返回
                total++;
                return total;
            }
        }).setParallelism(4);

        result.print();

    }
}
