package day0414.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class DistributedCacheDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //在该环境中，注册一个数据
        //可以是本地，可以使HDFS
        env.registerCachedFile("D:\\temp\\data.txt","myfile");

        //执行一个计算
        DataSet<Integer> source = env.fromElements(1,2,3,4,5,6,7,8,9,10);

        //处理source的数据
        //MapFunction的实现类RichMapFunction
        source.map(new RichMapFunction<Integer, Object>() {

            private String cacheData ="";

            @Override
            public void open(Configuration parameters) throws Exception {
                //对该任务进行初始化
                //获取之前缓存的数据
                File file = this.getRuntimeContext().getDistributedCache().getFile("myfile");
                //得到文件内容
                List<String> list = FileUtils.readLines(file);
                cacheData = list.get(0);
            }

            @Override
            public Object map(Integer value) throws Exception {
                //会在TaskManager上执行
                return value+cacheData;
            }
        }).print();



    }
}
