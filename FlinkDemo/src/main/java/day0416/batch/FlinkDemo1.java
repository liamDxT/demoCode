package day0416.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlinkDemo1 {
    public static void main(String[] args) throws Exception {
        //测试接口Map、FlatMap与MapPartition
        //创建访问接口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataSet<String> source = env.fromElements("I love Beijing","I love China","Beijing is the capital of china");

        System.out.println("***********map执行结果**************");
        //map循环
        source.map(new RichMapFunction<String, List<String>>() {

            @Override
            public List<String> map(String s) throws Exception {
                //分词
                String[] words =s.split(" ");
                List<String> result = new ArrayList<String>();
                for(String w :words){
                    result.add("单词是:"+w);
                }
                result.add("******");
                return result;
            }
        }).print();
        System.out.println("***********map执行结果**************");
        /*
        [单词是:I, 单词是:love, 单词是:Beijing, ******]
        [单词是:I, 单词是:love, 单词是:China, ******]
        [单词是:Beijing, 单词是:is, 单词是:the, 单词是:capital, 单词是:of, 单词是:china, ******]
         */

        System.out.println("***********flatMap执行结果**************");

        //flatMap = flatten + map
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String w: words) {
                    collector.collect(w);
                }
                collector.collect("-------------------");

            }
        }).print();

        /*
        I
        love
        Beijing
        I
        love
        China
        Beijing
        is
        the
        capital
        of
        china
         */
        System.out.println("*************mapPartition执行结果************");
        //mapPartition：分区
        //第一个泛型:分区中的元素
        //第二个泛型;对分区中的元素处理后的结果
        source.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //针对分区进行操作
                //取出分区中的元素
                Iterator<String> its = values.iterator();
                while(its.hasNext()){
                    String line = its.next();
                    String[] words = line.split(" ");
                    for (String w :words) {
                        out.collect("分区中的元素:"+w);
                    }
                    out.collect("=========================");
                }

            }
        }).print();
        /*
        分区中的元素:I
        分区中的元素:love
        分区中的元素:Beijing
        =========================
        分区中的元素:I
        分区中的元素:love
        分区中的元素:China
        =========================
        分区中的元素:Beijing
        分区中的元素:is
        分区中的元素:the
        分区中的元素:capital
        分区中的元素:of
        分区中的元素:china
        =========================


         */

    }
}
