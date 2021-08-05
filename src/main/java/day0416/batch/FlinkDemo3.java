package day0416.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import scala.Tuple3;
import scala.math.Integral;

import java.util.ArrayList;

public class FlinkDemo3 {
    public static void main(String[] args) throws Exception {
        // Join操作
        // 访问接口：ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建两张表
        //1、用户表(tid,tname)
        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1,"Tom"));
        list1.add(new Tuple2<>(2,"Mike"));
        list1.add(new Tuple2<>(3,"Mary"));
        list1.add(new Tuple2<>(4,"Jone"));

        //2、地区表(tid、地区)
        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1,"北京"));
        list2.add(new Tuple2<>(2,"北京"));
        list2.add(new Tuple2<>(3,"上海"));
        list2.add(new Tuple2<>(4,"广州"));
        list2.add(new Tuple2<>(3,"深圳"));

       DataSet<Tuple2<Integer,String>> table1 =env.fromCollection(list1);
       DataSet<Tuple2<Integer,String>> table2 =env.fromCollection(list2);

       //执行join:等值连接
        //连接条件where(0).equalTo(0) 使用第一张表的一个列，连接第二张表的第一列
        table1.join(table2).where(0).equalTo(0)
                //JoinFunction的第一个泛型:第一张表
                //JoinFunction的第二个泛型:第二张表
                //JoinFunction的第三个泛型:输出结果Tuple3<Integer,String,String>
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer,String,String>(first.f0,first.f1,second.f1);
                    }
                }).print();



    }
}
