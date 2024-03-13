

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class FileReader {

    private final String filePath;
    private Properties properties;

    public FileReader(String filePath) {
        this.filePath = filePath;
    }

    private void loadFileProperties() {
        if (properties != null) {
            return; // Properties already loaded
        }
        try (FileInputStream configFileReader = new FileInputStream(filePath)) { //try-with
            properties = new Properties();
            properties.load(configFileReader);
            // file closes automatically
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties from file: " + filePath, e);
        }
    }

    public String getProperty(String propertyName) {
        if (properties == null) {
            loadFileProperties();
        }
        return properties.getProperty(propertyName);
    }
}


