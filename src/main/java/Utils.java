import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {

    public static Properties getProperties() throws IOException {

        //to load application's properties, we use this class
        Properties mainProperties = new Properties();

        FileInputStream file;

        //the base folder is ./, the root of the main.properties file
        String path = "./config.properties";

        //load the file handle for main.properties
        file = new FileInputStream(path);

        //load all the properties from this file
        mainProperties.load(file);

        //we have loaded the properties, so close the file handle
        file.close();

        return mainProperties;
    }

    public static String getAppBrokers() throws IOException {

        String versionString = null;

        Properties mainProperties = getProperties();

        //retrieve the property we are intrested, the app.version
        versionString = mainProperties.getProperty("brokers");

        return versionString;
    }

    public static String getAppTopicName() throws IOException {

        String versionString = null;

        Properties mainProperties = getProperties();

        //retrieve the property we are intrested, the app.version
        versionString = mainProperties.getProperty("topicName");

        return versionString;
    }

}
