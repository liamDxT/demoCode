package day0416.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class FlinkDemo4 {
    public static void main(String[] args) throws Exception {
        //笛卡尔积
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

        DataSet<Tuple2<Integer, String>> table1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> table2 = env.fromCollection(list2);

        //生成笛卡尔积
        table1.cross(table2).print();



    }
}
