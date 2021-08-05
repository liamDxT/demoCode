package day0416.batch;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import javax.imageio.spi.ImageReaderWriterSpi;
import java.util.ArrayList;
import java.util.Objects;

public class FlinkDemo6 {
    public static void main(String[] args) throws Exception {
        //创建访问接口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建两张表
        //1、用户表(tid,tname)
        ArrayList<Tuple2<Integer,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1,"Tom"));
        list1.add(new Tuple2<>(3,"Mary"));
        list1.add(new Tuple2<>(4,"Jone"));

        //2、地区表(tid、地区)
        ArrayList<Tuple2<Integer,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1,"北京"));
        list2.add(new Tuple2<>(2,"北京"));
        list2.add(new Tuple2<>(4,"广州"));


        DataSet<Tuple2<Integer, String>> table1 = env.fromCollection(list1);
        DataSet<Tuple2<Integer, String>> table2 = env.fromCollection(list2);

        //左外连接
        table1.leftOuterJoin(table2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {

                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
                            throws Exception {
                        //是一个左外连接  table1会包含  table2不会包含
                        if(second == null) {
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,null);
                        }else {
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                        }
                    }
                }).sortPartition(0, Order.ASCENDING).sortPartition(1,Order.ASCENDING).print();

        System.out.println("*******************");

    //右外连接
        table1.rightOuterJoin(table2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>,Tuple2<Integer, String>,Tuple3<Integer, String, String>>() {

                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
                            throws Exception {
                        //是一个右外连接  table2会包含  table1不会包含
                        if(first == null) {
                            return new Tuple3<Integer, String, String>(second.f0,null,second.f1);
                        }else {
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
        System.out.println("*******************");
            //全外连接
        table1.fullOuterJoin(table2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first ==null ){
                            return  new Tuple3<Integer, String, String>(second.f0,null,second.f1);
                        }else if (second ==null ){
                            return  new Tuple3<Integer, String, String>(first.f0,first.f1,null);
                        }else{
                            return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();



    }
}
