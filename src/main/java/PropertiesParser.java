import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

//Create java class named “PropertiesParser”
public class PropertiesParser {

    //This function extract the content from the config.properties file and returns a Properties object
    public static Properties getProperties(String propertyFile) throws IOException {
        //to load application's properties, we use this class
        Properties mainProperties = new Properties();

        FileInputStream file;

        //load the file handle for propertyFile
        file = new FileInputStream(propertyFile);

        //load all the properties from this file
        mainProperties.load(file);

        //we have loaded the properties, so close the file handle
        file.close();

        return mainProperties;
    }

    //This function take a tag name and a file name and return the value for the tag name from the file
    public static String getPropertyValueByName(String propertyFile, String name) throws IOException {
        String value;

        Properties mainProperties = getProperties(propertyFile);

        value = mainProperties.getProperty(name);

        return Objects.requireNonNullElse(value, "null");
    }

}
