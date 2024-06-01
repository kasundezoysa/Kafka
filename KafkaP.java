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
        String topicName="topic1";
       
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kfk.ucsc.cmb.ac.lk:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
     
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try{
            for (int i = 0; i < 2; i++) {
                String key=Integer.toString(i);
                String message = "Message#";
                ProducerRecord<String,String> r=new ProducerRecord<>(topicName,key,message);
             
                Future<RecordMetadata> future = producer.send(r);
                RecordMetadata metadata = future.get();
                System.out.println(metadata.topic());
                System.out.println("Sent: " + message);
            }
        } catch (Exception e){
            System.out.println(e);
        }
      //  producer.flush();
        producer.close();
       
    }
}
