import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaC{
    public static void main(String[] args) {
        String topicName="topic1";
        Properties props = new Properties();
     
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kfk.ucsc.cmb.ac.lk:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Change this value to "earliest", "latest", or "none" based on your use case
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");// Start reading from the beginning

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topicName));
        // Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed record with key %s and value %s%n", record.key(), record.value());
            }
         
        }
       
    }
}