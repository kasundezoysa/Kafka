import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaT{

    public static void main(String[] args) {
        String topicName = "topic1";
        int numPartitions = 3;
        short replicationFactor = 1;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.248.22.133:9092");

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Define a new topic
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            // Create the topic
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created successfully");

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
