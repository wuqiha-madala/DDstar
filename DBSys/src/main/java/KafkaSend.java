import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;


/**
 * Created by Liutao on 2019/5/13 16:16
 */
public class KafkaSend {
    private String topic;
    public KafkaSend(String topic){
        super();
        this.topic = topic;
    }

    public static void sendMessage(String topic , String key , String data){
        Producer<String, String> producer = createProducer();
        producer.send(new KeyedMessage<String , String>(topic , key , data));
    }

    private static Producer<String , String> createProducer(){
        Properties properties = new Properties();
        properties.put("metadata.broker.list" , "hadoop01:9092");
        properties.put("zookeeper.connect" , "hadoop01:2181");
        properties.put("serializer.class" , StringEncoder.class.getName());
        return new Producer<String, String>(new ProducerConfig(properties));
    }

}