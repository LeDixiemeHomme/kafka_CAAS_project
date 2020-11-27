
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
            topicName= PropertiesParser.getPropertyValueByName("topicName");
            partitions= PropertiesParser.getPropertyValueByName("partitions");
            startOption= PropertiesParser.getPropertyValueByName("startOption");
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }

        switch(getArgumentValue(args,"--action").toLowerCase()) {
            case "produce":
                if(topicName ==null){
                    try {
                        topicName = getArgumentValue(args,"--topicName").toLowerCase();
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
                        topicName = getArgumentValue(args,"--topicName").toLowerCase();
                    } catch (Exception e){
                        Message.NoTopicNameArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                }

                if(startOption == null){
                    try {
                        startOption = getArgumentValue(args,"--startOption").toLowerCase();
                    } catch (Exception e){
                        Message.NoStartOptionArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                }

                if(partitions == null){
                    try {
                        partitionNumbers = extractNumber(getArgumentValue(args,"--partitions"));
                    } catch (Exception e){
                        Message.NoPartitionsArgMessage();
                        Message.consumeMessage();
                        break;
                    }
                } else {
                    partitionNumbers = extractNumber(partitions);
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

    public static String getArgumentValue(String[] args, String value){
        if(Arrays.asList(args).contains(value)){
            return args[Arrays.asList(args).indexOf(value)+1];
        } else {
            return null;
        }
    }

    public static int[] extractNumber(String str) {
        int i = 0;
        String[] stringTab = str.toLowerCase().split(",");
        int[] tab = new int[stringTab.length];

        for (String value: str.toLowerCase().split(",")) {
            tab[i] = Integer.parseInt(value);
            i++;
        }
        return tab;
    }
}
