import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaP{
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.248.22.133:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
     //   props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try{
            for (int i = 0; i < 2; i++) {
                String key=Integer.toString(i);
                String message = "Message " + i;
                ProducerRecord r=new ProducerRecord<>("topic1",message);
                producer.send(r);
                Future<RecordMetadata> future = producer.send(r);
                RecordMetadata metadata = future.get();
                System.out.println(metadata);
                System.out.println("Sent: " + message);
            }
        } catch (Exception e){
            System.out.println(e);
        }
        producer.flush();
        producer.close();
       
    }
}
