import java.io.IOException;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumer {
    public static void consume(String brokers, String topicName, String startOption, int[] partitionNumbers) throws Exception {

        System.out.println("brokers = " + brokers);
        System.out.println("topicName = " + topicName);
        System.out.println("startOption = " + startOption);
        System.out.println("partitionNumbers = " + Arrays.toString(partitionNumbers));

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        Set<TopicPartition> partSet = new HashSet<TopicPartition>();

        for(int partitionNumber : partitionNumbers){
            partSet.add(new TopicPartition(topicName, partitionNumber));
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        switch(startOption) {
            case "from-beginning":
                consumer.assign(partSet);
                consumer.seekToBeginning(partSet);
                break;
            case "regular":
                consumer.assign(partSet);
                consumer.seekToEnd(partSet);
                break;
            case "manual":
                consumer.assign(partSet);

                System.out.println("Manual mode choose for this consumer.");

                Map<TopicPartition, Long> beginningOffsets;
                beginningOffsets = consumer.beginningOffsets(partSet);
                Map<TopicPartition, Long> endOffsets;
                endOffsets = consumer.endOffsets(partSet);

                System.out.println("Partitions state :");
                for (TopicPartition partition : partSet) {

                    Long beginningOffset = beginningOffsets.get(partition);
                    Long endOffset = endOffsets.get(partition);
                    System.out.println("    partition : " + partition.toString());
                    System.out.println("    This consumer offset for this partition : " + consumer.position(partition));
                    System.out.println("    This partition starts at this offset : " + beginningOffset);
                    System.out.println("    This partition ends at this offset : " + endOffset + "\n");

                    System.out.println("You can now choose the consumer offset");
                    System.out.println("Your options are :");
                    System.out.println("    - Type \"a number\" between " + beginningOffset + " and " + endOffset + "" +
                            " to set the offset.");
                    System.out.println("    - Type \"exit\" to quit.");

                    boolean exit = false;
                    while(!exit){
                        Scanner scanner = new Scanner( System.in );
                        System.out.print("> ");
                        String line = scanner.next();
                        if(line.equals("exit")){
                            System.out.println("The offset won't be changed for the partition " + partition.toString() + " .");
                            break;
                        } else if (Long.parseLong(line) <= endOffset && Long.parseLong(line) >= beginningOffset){
                            try
                            {
                                consumer.seek(partition, Long.parseLong(line));
                            }
                            catch (Exception ex)
                            {
                                System.out.print(ex.getMessage());
                                throw new IOException(ex.toString());
                            }
                            exit = true;
                        } else {
                            System.out.println("Wrong type of value typed !");
                        }
                    }
                }

                break;
            default:
                Message.NoRegistredValueForStartOptionArgMessage();
                Message.consumeMessage();
                consumer.subscribe(Collections.singletonList(topicName));
                break;
        }

//

        //Kafka Consumer subscribes list of topics here.
//        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);

        Duration duration = Duration.ofMillis(100);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(duration);
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
