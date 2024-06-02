import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaT {
   
    public static void main(String[] args) throws Exception{
       
        String topic1 = "topic1";
        int numPartitions = 3;
        short replicationFactor = 1;    

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kfk.ucsc.cmb.ac.lk:9092");
        // Create an AdminClient
        AdminClient adminClient = AdminClient.create(props);

        // Define a new topic
        NewTopic newTopic = new NewTopic(topic1, numPartitions, replicationFactor);
        // Create the topic
        CreateTopicsResult resultC =adminClient.createTopics(Collections.singleton(newTopic));
        KafkaFuture<Void>  f1=resultC.all();
        f1.get(); //Wait for completion
        System.out.println(f1);
        if(f1.isDone()) System.out.println("Topic created successfully");

        
        // Delete the topic
        String topic2 = "topic1";
        List<String> topicList = new ArrayList<String>();
        topicList.add(topic2);
        DeleteTopicsResult resultD= adminClient.deleteTopics(topicList);
        KafkaFuture<Void> f2=resultD.all();
        f2.get(); //Wait for completion
        if(f2.isDone()) System.out.println(f2);
        System.out.println("Topic deleted successfully");
    }

}