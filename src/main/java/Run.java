
import java.util.Hashtable;

//Create java class named “Run”, this class contains the main function
public class Run {

    public static void main(String[] args) throws Exception {

        //Create a String[] to store arguments name
        String[] argumentTags = {"action", "brokers", "topicName", "partitions", "startOption", "elasticsearch"};

        //Create a String to store pathPropertyFile argument value or " " if this argument isn't in the command
        String path = Tools.getArgumentValueByTag(args, "pathPropertyFile");

        //Create a Hashtable<String, String> to store arguments name and its value
        Hashtable<String, String> argumentsPlusValue;

        argumentsPlusValue = Tools.populateArgumentHashtable(args, argumentTags, path);
        Tools.argumentValueChecker(argumentsPlusValue);

        //Initiate a switch for every action value possible : produce | consume | help
        switch(argumentsPlusValue.get("action").toLowerCase()) {
            case "produce":
                SimpleProducer.produce(argumentsPlusValue);
                break;
            case "consume":
                SimpleConsumer.consume(argumentsPlusValue);
                break;
            case "help":
                Message.helpMessage();
                break;
            default:
                Message.defaultMessage();
                break;
        }
    }
}
