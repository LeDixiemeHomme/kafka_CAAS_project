//import util.properties packages
import java.io.IOException;
import java.util.*;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//import PartitionInfo packages
import org.apache.kafka.common.PartitionInfo;

//import TopicPartition packages
import org.apache.kafka.common.TopicPartition;

//Create java class named “SimpleProducer”
public class SimpleProducer {

    //This function processes the Hashtable<String, String> and starts a KafkaProducer according to arguments values
    public static void produce(Hashtable<String, String> argumentsPlusValue) throws Exception{

        //Create a int[] to store partition number from the string argument "partitions"
        int[] partitionNumbers = Tools.extractNumber(argumentsPlusValue.get("partitions"));

        //Create a Properties object used to create the KafkaProducer object
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", argumentsPlusValue.get("brokers"));

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Set<TopicPartition> partSet = new HashSet<>();

        //Iterate for each number in the partitionNumber tab
        for(int partitionNumber : partitionNumbers){
            //Fill a Set<TopicPartition> entry with the topicName and the current partition number
            partSet.add(new TopicPartition(argumentsPlusValue.get("topicName"), partitionNumber));
        }

        //Create a KafkaProducer object with props
        Producer<String, String> producer = new KafkaProducer<>(props);

        //Create a PartitionInfo list which will contains every partition inside the topic aimed
        List<PartitionInfo> list = producer.partitionsFor(argumentsPlusValue.get("topicName"));

        Message.messageEveryPartitionInTopic(argumentsPlusValue.get("topicName"), list, partSet);

        Message.messageChoiceProducerInput();

        //Start while loop to get user inputs
        boolean sortie = false;
        while(!sortie){
            Scanner scanner = new Scanner( System.in );
            System.out.print("> ");
            //Change delimiter " " to "\n", it will allows the user to input " " in his message input
            scanner.useDelimiter("\n");
            String line = scanner.next();
            if(line.equals("exit")){
                //If the input equals to "exit" the program stop
                sortie = true;
            } else {
                //If the input isn't equals to "exit" the program stop
                try {
                    //Iterate for each partition number inside the int[] partitionNumbers
                    for (int num: partitionNumbers) {
                        //Create a ProducerRecord object with the topic name, the current partition number, a key value equals to null and the user input
                        ProducerRecord record = new ProducerRecord<String, String>(argumentsPlusValue.get("topicName"),num, null,line);
                        //Asynchronously send the ProducerRecord object record to the topic
                        producer.send(record).get();
                        System.out.println("Message sent successfully to partition "+ num + ".");
                    }
                }
                catch (Exception ex)
                {
                    System.out.print(ex.getMessage());
                    throw new IOException(ex.toString());
                }
            }
        }
        //Close the producer
        producer.close();
    }
}
