package day0414.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 广播变量，类似于分布式缓存
 * 创建一个广播对象List peoples 将广播对象转换成hashmap  broadCast
 * 创建处理的数据源source 遍历map source 同事获取withBroadcastSet broadCast广播出去 名称为mydata
 * map source时 通过广播name 名称为mydata 获取broadCast 放入到hashmap中
 * 通过source的名字获取 broadCast中的年龄 拼接输出
 *
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建需要广播的变量(对象):人的信息(姓名，年龄)
        List<Tuple2<String,Integer>> peoples = new ArrayList<>();
        peoples.add(new Tuple2<String,Integer>("Tome",23));
        peoples.add(new Tuple2<String,Integer>("Mike",24));
        peoples.add(new Tuple2<String,Integer>("Mary",24));

        DataSet<Tuple2<String,Integer>> peoplesData =env.fromCollection(peoples);

        //把上面的list改为hashmap
      DataSet<HashMap<String,Integer>> broadCast = peoplesData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String,Integer>>() {


            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
               HashMap<String,Integer> result = new HashMap<>();
               result.put(value.f0,value.f1);
                return result;
            }
        });

      //需要处理的数据集合
        //创建数据源
        DataSet<String> source = env.fromElements("Tome","Mike","Mary");

        source.map(new RichMapFunction<String, String>() {
            //定义一个变量保存广播变量的值
            private HashMap<String,Integer> mydata = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //读取广播变量的值
                //取出广播变量broadCast
                List<HashMap<String,Integer>> list = getRuntimeContext().getBroadcastVariable("mydata");
                for(HashMap<String,Integer> l :list){
                    mydata.putAll(l);
                }

            }

            @Override
            public String map(String name) throws Exception {
                //获取名字获取年龄
                Integer age = mydata.get(name);
                return "姓名:"+name+", 年龄:"+age;
            }
        }).withBroadcastSet(broadCast,"mydata").print();


    }
}
