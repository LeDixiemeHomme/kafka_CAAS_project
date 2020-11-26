import java.io.IOException;
import java.util.*;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

        List<TopicPartition> partitions = new ArrayList<>();

        for(int partitionNumber : partitionNumbers){
            partitions.add(new TopicPartition(topicName, partitionNumber));
        }

        partitions.toString();

        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);


        switch(startOption) {
            case "from-beginning":
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                break;
            case "regular":
                consumer.assign(partitions);
                consumer.seekToEnd(partitions);
                break;
            default:
                Message.NoRegistredValueForStartOptionArgMessage();
                Message.consumeMessage();
                consumer.subscribe(Collections.singletonList(topicName));
                break;
        }



//        consumer.assign(partitions);
//
//        consumer.seek(new TopicPartition(topicName, 0), 1);
//        consumer.seek(new TopicPartition(topicName, 1), 1);

        //Kafka Consumer subscribes list of topics here.
//        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
