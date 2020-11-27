import java.util.Arrays;

public class Tools {

    public static boolean isNumeric(String line){
        try
        {
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


    public static String getArgumentValue(String[] args, String value){
        if(Arrays.asList(args).contains(value)){
            return args[Arrays.asList(args).indexOf(value)+1];
        } else {
            return " ";
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
