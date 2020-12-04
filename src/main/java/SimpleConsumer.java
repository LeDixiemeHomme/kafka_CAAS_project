//import util.properties packages
import java.io.IOException;
import java.time.Duration;
import java.util.*;

//import KafkaProducer packages
import org.apache.kafka.clients.consumer.KafkaConsumer;

//import ConsumerRecord and ConsumerRecords packages
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

//import TopicPartition packages
import org.apache.kafka.common.TopicPartition;

//Create java class named “SimpleConsumer”
public class SimpleConsumer {

    //This function processes the Hashtable<String, String> and starts a KafkaConsumer according to arguments values
    public static void consume(Hashtable<String, String> argumentsPlusValue) throws Exception {

        //Create a int[] to store partition number from the string argument "partitions"
        int[] partitionNumbers = Tools. extractNumber(argumentsPlusValue.get("partitions"));

        //Create a Properties object used to create the KafkaConsumer object
        Properties props = new Properties();

        props.put("bootstrap.servers", argumentsPlusValue.get("brokers"));
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        Set<TopicPartition> partSet = new HashSet<>();

        //Iterate for each number in the partitionNumber tab
        for(int partitionNumber : partitionNumbers){
            //Fill a Set<TopicPartition> entry with the topicName and the current partition number
            partSet.add(new TopicPartition(argumentsPlusValue.get("topicName"), partitionNumber));
        }

        //Create a KafkaConsumer object with props
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //Assign to the KafkaConsumer object every TopicPartition inside the Set<TopicPartition> partSet
        consumer.assign(partSet);

        //Initiate a switch for every startOption value possible : regular | from-beginning | manual
        switch(argumentsPlusValue.get("startOption")) {
            case "from-beginning":
                System.out.println("From beginning mode choose for this consumer.");
                //Seek to the first offset for each of the given partitions inside the Set<TopicPartition> partSet
                consumer.seekToBeginning(partSet);
                break;
            case "regular":
                System.out.println("Regular mode choose for this consumer.");
                //Seek to the last offset for each of the given partitions inside the Set<TopicPartition> partSet
                consumer.seekToEnd(partSet);
                break;
            case "manual":
                System.out.println("Manual mode choose for this consumer.");

                Map<TopicPartition, Long> beginningOffsets;
                Map<TopicPartition, Long> endOffsets;

                //Get the first offset for the given partitions inside the Set<TopicPartition> partSet
                beginningOffsets = consumer.beginningOffsets(partSet);
                //Get the end offsets for the given partitions inside the Set<TopicPartition> partSet
                endOffsets = consumer.endOffsets(partSet);

                System.out.println("Partitions state :");

                //Iterate for each partitions inside the Set<TopicPartition> partSet
                for (TopicPartition partition : partSet) {

                    //Get the first offset for the current partition from the Map<TopicPartition, Long> beginningOffsets
                    Long beginningOffset = beginningOffsets.get(partition);
                    //Get the last offset for the current partition from the Map<TopicPartition, Long> endOffsets
                    Long endOffset = endOffsets.get(partition);

                    Message.partitionStateMessage(partition.toString(), consumer.position(partition), beginningOffset, endOffset);

                    Message.messageChoiceConsumerOffset(beginningOffset, endOffset);

                    //Start while loop to get user inputs
                    boolean exit = false;
                    while(!exit){
                        Scanner scanner = new Scanner( System.in );
                        System.out.print("> ");
                        String line = scanner.next();
                        if(line.equals("exit")){
                            //If the input equals to "exit" the consumer seek to the last offset the current partition
                            System.out.println("The offset won't be changed for the partition : " + partition.toString() + " .");
                            break;
                        } else if (Tools.isNumeric(line)) {
                            //If the isNumeric function return true with the user input
                            if (Long.parseLong(line) <= endOffset && Long.parseLong(line) >= beginningOffset){
                                //If the input is between the beginningOffset value and the endOffset value
                                try {
                                    //Overrides the fetch offsets that the consumer will use on the next poll and replace it with the user input
                                    consumer.seek(partition, Long.parseLong(line));
                                }
                                catch (Exception ex)
                                {
                                    System.out.print(ex.getMessage());
                                    throw new IOException(ex.toString());
                                }
                                exit = true;
                            } else {
                                //Display messages if the user input isn't between the beginningOffset value and the endOffset value
                                System.out.println("The number typed is out of range.");
                                System.out.println("Try again or type exit.");
                            }
                        } else {
                            //Display messages if the user input isn't between the beginningOffset value and the endOffset value or isn't "exit"
                            System.out.println("The value typed is neither \"exit\" nor a number.");
                            System.out.println("Try again or type exit.");
                        }
                    }
                }

                break;
            default:
                //Display messages if the value for the argument startOption isn't regular | from-beginning | manual
                Message.unrecognizedValueForStartOptionArgMessage();
                Message.consumeMessage();
                consumer.subscribe(Collections.singletonList(argumentsPlusValue.get("topicName")));
                break;
        }

        //print the topic name
        System.out.println("Subscribed to topic " + argumentsPlusValue.get("topicName"));

        Duration duration = Duration.ofMillis(100);

        while (true) {
            /* Fetch data for the topics or partitions specified using one of the subscribe/assign APIs until duration value,
            then retries to fetch */
            ConsumerRecords<String, String> records = consumer.poll(duration);
            //Iterate for each ConsumerRecord fetched
            for (ConsumerRecord<String, String> record : records)
                // print the offset,key and value for the consumer records.
                System.out.printf("topic = %s; partition = %d; offset = %d; key = %s; value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
    }
}
