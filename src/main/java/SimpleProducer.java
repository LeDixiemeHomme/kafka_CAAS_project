//import util.properties packages
import java.io.IOException;
import java.util.*;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.swing.text.StyledEditorKit;

//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void produce(String brokers, String topicName, int[] partitionNumbers) throws Exception{

        System.out.println("brokers = " + brokers);
        System.out.println("topicName = " + topicName);
        System.out.println("partitionNumbers = " + Arrays.toString(partitionNumbers));

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", brokers);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Set<TopicPartition> partSet = new HashSet<>();

        for(int partitionNumber : partitionNumbers){
            partSet.add(new TopicPartition(topicName, partitionNumber));
        }

        Producer<String, String> producer = new KafkaProducer<>(props);

        List<PartitionInfo> list = producer.partitionsFor(topicName);

        System.out.println("Your messages will be sent to the topic : " + topicName);
        for (PartitionInfo info : list) {
            for (TopicPartition partition : partSet) {
                if(partition.partition() == info.partition()) {
                    System.out.println("At this partition : " + partition.toString());
                }
            }
        }

        System.out.println("You can now type your message.");
        System.out.println("Your options are :");
        System.out.println("    - Type \"a string\".");
        System.out.println("    - Type \"exit\" to quit.");

        boolean sortie = false;

        while(!sortie){

            Scanner scanner = new Scanner( System.in );
            System.out.print("> ");
            scanner.useDelimiter("\n");
            String line = scanner.next();
            if(line.equals("exit")){
                sortie = true;
            } else {
                try
                {
                    System.out.println(line);
                    for (int num: partitionNumbers) {
                        producer.send(new ProducerRecord<String, String>(topicName,num, null,line)).get();
                    }
                }
                catch (Exception ex)
                {
                    System.out.print(ex.getMessage());
                    throw new IOException(ex.toString());
                }
                System.out.println("Message sent successfully");
            }
        }
        producer.close();
    }
}
