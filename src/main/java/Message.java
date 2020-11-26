public class Message {

    public static void helpMessage() {
        System.out.println("Help Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --action : produce | consume | help");
    }

    public static void produceMessage() {
        System.out.println("Produce Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --topicName : \"the topic name\"");
    }

    public static void consumeMessage() {
        System.out.println("Consume Message :");
        System.out.println("    Possible arguments :");
        System.out.println("    --topicName : \"the topic name\"");
        System.out.println("    --partitions : 0,1,2 for example");
        System.out.println("    --startOption : regular | from-beginning | manual");
    }

    public static void NoPartitionsArgMessage() {
        System.out.println("No partition found neither in the config.properties file nor in argument. " +
            "Try to use the --partitions argument followed by numbers inside brackets separated by comma like this [0,1,2] for example.");
    }

    public static void NoStartOptionArgMessage() {
        System.out.println("No start option found neither in the config.properties file nor in argument. " +
                "Try to use the --startOption argument followed by the topic name.");
    }

    public static void NoRegistredValueForStartOptionArgMessage() {
        System.out.println("No registred value for --startOption argument, default startOption used.");
    }

    public static void NoTopicNameArgMessage() {
        System.out.println("No topic name found neither in the config.properties file nor in argument. " +
                "Try to use the --topicName argument followed by the topic name.");
    }
}
