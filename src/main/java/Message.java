import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;

//Create java class named “Message”
public class Message {

    //Display a help message that tells how to give parameters to the program
    public static void helpMessage() {
        System.out.println("Help Message :");
        System.out.println("   This program use KAFKA and can generate a producer or/and a consumer. \n" +
                "   It needs those arguments to work :\n" +
                "       - action (produce | consume | help.)\n" +
                "       - brokers (ip address + port : the ip address and port of your broker.)\n" +
                "       - topicName (string : the name of your topic.)\n" +
                "       - partitions (int,int : number separated with coma of the partitions you want to interacte with.)\n" +
                "       - (consumer only) startOption (regular | from-beginning | manual.)\n");
        System.out.println("    You can either :\n" +
                "       - create a config.properties file and specify the path to this file with the tag --pathPropertyFile. \n" +
                "       - give them inside the command with the pattern : --argument_tag argument_value.");
    }

    //Display a default message that display possible arguments value for the action tag
    public static void defaultMessage() {
        System.out.println("Default Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --action : produce | consume | help");
    }

    //Display a produce message that display possible arguments value when the action tag value equals produce
    public static void produceMessage() {
        System.out.println("Produce Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --brokers : brocker address:broker port (127.0.0.1:9092)");
        System.out.println("    --topicName : \"the topic name\"");
        System.out.println("    --partitions : 0,1,2 for example");
    }

    //Display a consume message that display possible arguments value when the action tag value equals consume
    public static void consumeMessage() {
        System.out.println("Consume Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --brokers : brocker address:broker port (127.0.0.1:9092)");
        System.out.println("    --topicName : \"the topic name\"");
        System.out.println("    --partitions : 0,1,2 for example");
        System.out.println("    --startOption : regular | from-beginning | manual");
    }

    //Display a no partitions arg message that display the procedure to add the partitions parameter
    public static void noPartitionsArgMessage() {
        System.out.println("No partition found neither in the config.properties file nor in argument. " +
            "Try to use the --partitions argument followed by numbers inside brackets separated by comma like this [0,1,2] for example.");
    }

    //Display a no start option arg message that display the procedure to add the startOption parameter
    public static void noStartOptionArgMessage() {
        System.out.println("No start option found neither in the config.properties file nor in argument. " +
                "Try to use the --startOption argument followed by regular | from-beginning | manual.");
    }

    /*Display a unrecognized value for start option arg message
    that display to the user that an unrecognized value was use for the tag startOption*/
    public static void unrecognizedValueForStartOptionArgMessage() {
        System.out.println("Unrecognized value for --startOption argument, default startOption used.");
    }

    //Display a no topic name arg message that display the procedure to add the topicName parameter
    public static void noTopicNameArgMessage() {
        System.out.println("No topic name found neither in the config.properties file nor in argument. " +
                "Try to use the --topicName argument followed by the topic name.");
    }

    //Display a no brokers arg message that display the procedure to add the brokers parameter
    public static void noBrokersArgMessage() {
        System.out.println("No brokers address and port found neither in the config.properties file nor in argument. " +
                "Try to use the --brokers argument followed by the brokers address and port.");
    }

    //Display a no action arg message that display the procedure to add the action parameter
    public static void noActionArgMessage() {
        System.out.println("No action found neither in the config.properties file nor in argument. " +
                "Try to use the --action argument followed by produce | consume | help");
    }

    //Display a partition state message that display information about the state of the partition
    public static void partitionStateMessage(String partition, Long position, Long beginningOffset, Long endOffset) {
        //Display information about the state of the current partition
        System.out.println("    partition : " + partition);
        System.out.println("    The consumer offset for this partition : " + position);
        System.out.println("    This partition starts at the offset : " + beginningOffset);
        System.out.println("    This partition ends at the offset : " + endOffset + "\n");
    }

    //Display a message choice consumer offset
    public static void messageChoiceConsumerOffset(Long beginningOffset, Long endOffset) {
        //Display choice options for consumer offset
        System.out.println("You can now choose the consumer offset.");
        System.out.println("Your options are :");
        System.out.println("    - Type \"a number\" between " + beginningOffset + " and " + endOffset + "" +
                " to set the offset.");
        System.out.println("    - Type \"exit\" to quit.");
    }

    //Display a message every partition in topic
    public static void messageEveryPartitionInTopic(String topicName, List<PartitionInfo> list, Set<TopicPartition> partSet) {
        //Display each partition inside the topic aimed
        System.out.println("Your messages will be sent to the topic : " + topicName);
        for (PartitionInfo info : list) {
            for (TopicPartition partition : partSet) {
                if(partition.partition() == info.partition()) {
                    System.out.println("At this partition : " + partition.toString());
                }
            }
        }
    }

    //Display a message choice producer input
    public static void messageChoiceProducerInput() {
        //Display choice options for producer input
        System.out.println("You can now type your message.");
        System.out.println("Your options are :");
        System.out.println("    - Type \"a string\".");
        System.out.println("    - Type \"exit\" to quit.");
    }

}
