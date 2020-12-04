import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

//Create java class named “Tools”, this class contains usefull function that can be used in every other class
public class Tools {

    //This function returns true if the String parameter can be parse into an int, false if not
    public static boolean isNumeric(String line){
        try {
            // checking valid integer using parseInt() method 
            Integer.parseInt(line);
            return true;
        }
        catch (NumberFormatException e)
        {
            System.out.println(line + " is not a valid integer number");
            return false;
        }
    }

    //This function returns the value of the argument tag given in parameter
    public static String getArgumentValueByTag(String[] args, String argTag){
        if(Arrays.asList(args).contains("--" + argTag)){
            return args[Arrays.asList(args).indexOf("--" + argTag)+1];
        } else {
            return "null";
        }
    }

    //This function returns an String without comma
    public static String deleteComma(String str) {
        return str.replace(",", "");
    }

    //This function returns an int[] based on a String given in parameter
    public static int[] extractNumber(String str) {
        String[] stringTab;
        stringTab = str.toLowerCase().split("");
        int[] tab = new int[stringTab.length];
        try {
            int i = 0;
            for (String value: stringTab) {
                tab[i] = Integer.parseInt(value);
                i++;
            }
        } catch (NumberFormatException e) {
            System.out.println(str + " is not a valid partition number argument");
        }
        return tab;
    }

    //this function return a Hashtable<String, String> filled with the argument tag and its value
    public static Hashtable<String, String> populateArgumentHashtable(String[] args, String[] argumentTags, String path) {

        Hashtable<String, String> argumentsPlusValue = new Hashtable<>();

        //Iterate for each argument inside the String[] arguments
        for (String argumentName: argumentTags) {
            if(!path.equals("null")) {
                try {
                    /*Put inside the Hashtable<String, String> argumentsPlusValue the current argumentName
                    and its value if it exists from the config.properties file, the string "null" if not */
                    String propertyValue;
                    if(argumentName.equals("partitions")) {
                        propertyValue = deleteComma(PropertiesParser.getPropertyValueByName(path, argumentName));
                    } else {
                        propertyValue = PropertiesParser.getPropertyValueByName(path, argumentName);
                    }
                    argumentsPlusValue.put(argumentName, propertyValue);
                }
                catch (IOException ioe){
                    ioe.printStackTrace();
                }
            } else {
                /*Put inside the Hashtable<String, String> argumentsPlusValue the current argumentName
                and its value if it exists from the command line, "null" if not */
                String argumentValue;
                if(argumentName.equals("partitions")) {
                    argumentValue = deleteComma(getArgumentValueByTag(args, argumentName));
                } else {
                    argumentValue = getArgumentValueByTag(args, argumentName);
                }
                argumentsPlusValue.put(argumentName, argumentValue);
            }
        }
        return argumentsPlusValue;
    }

    /*This function check if all parameters possess a value that isn't "null".
    If the value is "null" the value display a message and terminate the program with the exit code 0*/
    public static void argumentValueChecker(Hashtable<String, String> argumentsPlusValue) {

        if(argumentsPlusValue.get("action").equals("help")) {
            return;
        }

        boolean exit = false;
        for (Map.Entry<String, String> argument: argumentsPlusValue.entrySet()) {

            if(argument.getValue().equals("null") && !(argument.getKey().equals("action") || (argumentsPlusValue.get("action").equals("produce") && argument.getKey().equals("startOption")))) {
                switch (argument.getKey()){
                    case "action" :
                        Message.noActionArgMessage();
                        break;
                    case "brokers" :
                        Message.noBrokersArgMessage();
                        break;
                    case "topicName":
                        Message.noTopicNameArgMessage();
                        break;
                    case "partitions":
                        Message.noPartitionsArgMessage();
                        break;
                    case "startOption":
                        Message.noStartOptionArgMessage();
                        break;
                    default:
                        System.out.println("Unrecognized parameter.");
                        break;
                }
                exit = true;
            }
        }

        if(exit) {
            switch (argumentsPlusValue.get("action")) {
                case "produce":
                    Message.produceMessage();
                    break;
                case "consume":
                    Message.consumeMessage();
                    break;
                case "help":
                    Message.helpMessage();
                    break;
                default:
                    Message.defaultMessage();
                    break;
            }
            System.out.println("\nEnd of the programme.");
            System.exit(0);
        }
    }

}
