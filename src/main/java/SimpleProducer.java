//import util.properties packages
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.text.StyledEditorKit;

//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void produce(String brokers, String topicName) throws Exception{

//        String brokers = null;
//        String topicName = null;

//        try{
//            brokers = Utils.getAppBrokers();
//            topicName= Utils.getAppTopicName();
//        }
//        catch (IOException ioe) {
//            ioe.printStackTrace();
//        }
        System.out.println("brokers = " + brokers);
        System.out.println("topicName = " + topicName);

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", brokers);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        boolean sortie = false;

        while(!sortie){

            Scanner scanner = new Scanner( System.in );
            String line = scanner.next();
            if(line.equals("exit")){
                sortie = true;
            } else {
                try
                {
                    producer.send(new ProducerRecord<String, String>(topicName,
                            line)).get();
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
