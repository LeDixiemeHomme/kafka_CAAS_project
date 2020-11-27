
import java.io.IOException;
import java.util.Arrays;

public class Run {
    public static void main(String[] args) throws Exception {

        String brokers = null;
        String topicName = null;
        String partitions = null;
        String startOption = null;

        try{
            brokers = PropertiesParser.getPropertyValueByName("brokers");
//            topicName= PropertiesParser.getPropertyValueByName("topicName");
//            partitions= PropertiesParser.getPropertyValueByName("partitions");
//            startOption= PropertiesParser.getPropertyValueByName("startOption");
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }

        switch(Tools.getArgumentValue(args,"--action").toLowerCase()) {
            case "produce":
                if(topicName ==null){
                    try {
                        topicName = Tools.getArgumentValue(args,"--topicName").toLowerCase();
                        SimpleProducer.produce(brokers, topicName);
                    } catch (Exception e){
                        Message.NoTopicNameArgMessage();
                        break;
                    }
                }

                SimpleProducer.produce(brokers, topicName);
                break;
            case "consume":
                int[] partitionNumbers = null;
                if(topicName == null){
                    try {
                        topicName = Tools.getArgumentValue(args,"--topicName").toLowerCase();
                    } catch (Exception e){
                        Message.NoTopicNameArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                }

                if(startOption == null){
                    try {
                        startOption = Tools.getArgumentValue(args,"--startOption").toLowerCase();
                    } catch (Exception e){
                        Message.NoStartOptionArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                }

                if(partitions == null){
                    try {
                        partitionNumbers = Tools.extractNumber(Tools.getArgumentValue(args,"--partitions"));
                    } catch (Exception e){
                        Message.NoPartitionsArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                } else {
                    partitionNumbers = Tools.extractNumber(partitions);
                }

                SimpleConsumer.consume(brokers, topicName, startOption, partitionNumbers);
                break;
            case "help":
                Message.helpMessage();
                break;
            default:
                Message.helpMessage();
                break;
        }
    }
}
