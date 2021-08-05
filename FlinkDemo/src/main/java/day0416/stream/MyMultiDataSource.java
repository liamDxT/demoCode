package day0416.stream;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MyMultiDataSource implements ParallelSourceFunction<Integer> {

    private  int count = 0;
    //开关，控制数据源是否继续产生数据
    private  boolean isRunning = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        //如何产生数据
        while (isRunning){
            //每隔一秒产生一个整数
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning =false;
    }
}
