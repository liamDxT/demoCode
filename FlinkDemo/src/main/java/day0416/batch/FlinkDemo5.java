package day0416.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class FlinkDemo5 {
    public static void main(String[] args) throws Exception {
        //First-N 也叫Top-N

        //创建接口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //构造一张表:姓名，薪水，部门号
        DataSource<Tuple3<String, Integer, Integer>> source = env.fromElements(
                new Tuple3<String, Integer, Integer>("SMITH", 1000, 10),
                new Tuple3<String, Integer, Integer>("KING", 5000, 10),
                new Tuple3<String, Integer, Integer>("FORD", 3000, 20),
                new Tuple3<String, Integer, Integer>("JONE", 2500, 30),
                new Tuple3<String, Integer, Integer>("CLARK", 1000, 10)
        );
        //按照插入顺序，取出前三条记录
        source.first(3).print();
        System.out.println("***********");

        //先按照部门号排序，再按照薪水排序
        source.sortPartition(2, Order.ASCENDING).sortPartition(1,Order.DESCENDING).print();

        System.out.println("****************");

        //按照部门号分组，取出分组中的第一条数据
        source.groupBy(2).first(1).print();


    }
}
