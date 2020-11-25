
import java.io.IOException;
import java.util.UUID;

public class Run {
    public static void main(String[] args) throws Exception {

        String brokers = null;
        String topicName = null;

        try{
            brokers = Utils.getAppBrokers();
            topicName= Utils.getAppTopicName();
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }

        switch(args[0].toLowerCase()) {
            case "produce":
                SimpleProducer.produce(brokers, topicName);
                break;
            case "consume":
                // Either a groupId was passed in, or we need a random one
                String groupId;
                if (args.length == 4) {
                    groupId = args[3];
                } else {
                    groupId = UUID.randomUUID().toString();
                }
                SimpleConsumer.consume(brokers, topicName);
                break;
            default:
                SimpleProducer.produce(brokers, topicName);
                break;
        }
    }
}
