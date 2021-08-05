package day0416.stream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

//每隔一秒产生一个整数
public class MySingleDataSource implements SourceFunction<Integer> {

    private int count =0;
    //开关:控制数据源是否继续产生数据
    private boolean isRuning = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        //如何产生数据
        while (isRuning){
            //每隔一秒产生一个整数
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
            isRuning = false;
    }
}
