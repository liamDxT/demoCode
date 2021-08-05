package day0419.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkWithKafka {
    public static void main(String[] args) throws Exception {
        //创建访问接口
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建kafka配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata111:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"mygroup");

        //创建一个消费者客户端
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<String>("mytopic1",new SimpleStringSchema(),props);

        sEnv.addSource(source).print();
        sEnv.execute("FlinkWithKafka");

    }
}
