package cn.thinkingdata.tga.javasdk;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.util.Map;
import java.util.Properties;

/**
 * Created by quanjie on 2018/4/16.
 */
public class ProduceKafka implements Consumer {
    private Properties props = new Properties();
    private Producer<String,String> producer;
    private String topic;

    public ProduceKafka(String Server, String topic) {
        props.put("bootstrap.servers", Server);
        props.put("acks", "1");
        props.put("retries", 5);
        props.put("linger.ms", 3);
        props.put("batch.size", 50000);
        props.put("compression.type","gzip");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.topic = topic;
        producer = new KafkaProducer<>(props);
    }

    public ProduceKafka(String Server){
        this(Server,"tga_data_collector");
    }

    @Override
    public void add(Map<String, Object> message) {
        try {
            String value = JSON.toJSONStringWithDateFormat(message, "yyyy-MM-dd HH:mm:ss.SSS");
            producer.send(new ProducerRecord<String, String>(this.topic, value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to add data.", e);
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        this.flush();
        producer.close();
    }

    public void setProps(String key,Object value){
        this.props.put(key,value);
    }

    public void setTopic(String topic){
        this.topic = topic ;
    }
}
