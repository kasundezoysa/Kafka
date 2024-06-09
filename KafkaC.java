import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaC{
    public static void main(String[] args) {
        
        //Create a list of topics
        Collection<String> topics = new ArrayList<String>();
        topics.add("topic1"); 
        topics.add("topic5");

        
        //Create Consumer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kfk.ucsc.cmb.ac.lk:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group12");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Change this value to "earliest", "latest", or "none" based on your use case
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic/list of topics
        // consumer.subscribe(Collections.singletonList(topicName));
        consumer.subscribe(topics);

        // Poll to ensure the consumer joins the group and gets assigned partitions
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
    
        // Get the partition assignments for the consumer
        Set<TopicPartition> partitions = consumer.assignment();

        // If partition is empty, try again until cosumer receivers a partition assinment
        while (partitions.isEmpty()) {
            System.out.println("Empty partition, trying again..");
            consumer.poll(Duration.ofMillis(1000));
            partitions = consumer.assignment();
        }
 
        // Seek to the beginning of each partitions
        for (TopicPartition partition : partitions) {
            System.out.println("Partitions "+partition.topic()+"\n");
            consumer.seekToBeginning(Arrays.asList(partition)); 
        }

        // Poll for new data
        while (true) {
           records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed record with key %s and value %s from the topic %s%n",record.key(), record.value(),record.topic() );
            }
        
        }
    
    }
}