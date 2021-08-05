package day0416.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * 有累加器执行结果
 * 缺点:执行效率会降低
 * 无论并行度设置为多少，结果是:4
 */
public class AccumulateDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataSet<String> source=env.fromElements("Tom","Mary","Mike","Jone");
        //统计集合中的个数
       DataSet<Integer> result =source.map(new RichMapFunction<String, Integer>() {
            //定义一个累加器
            //注册到任务中
            private IntCounter intCount = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //将累加器注册到任务中
                this.getRuntimeContext().addAccumulator("myaccumulator",intCount);
            }

            @Override
            public Integer map(String s) throws Exception {
                //具体任务中操作累加器
                this.intCount.add(1);
                return 0;
            }
        }).setParallelism(8);
        result.writeAsText("d:\\temp\\abc8.txt");

        //获取累加器的值
       JobExecutionResult finalResult = env.execute("AccumulateDemo");
        int total = finalResult.getAccumulatorResult("myaccumulator");

        System.out.println("结果是:" + total );

    }


}
