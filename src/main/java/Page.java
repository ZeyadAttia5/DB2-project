


import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
//    private Vector<Tuple> tuples;
    private static final String configFilePath = "src/main/resources/DBApp.config";
    private int maximumRowsCountinPage;
    private int numberOfPages = 0;

    private FileReader configFileReader;

    public Page() {
        this.configFileReader = new FileReader(configFilePath);
        this.maximumRowsCountinPage = Integer.parseInt(configFileReader.getProperty("MaximumRowsCountinPage"));
        System.out.println("Maximum number of rows per page: " + maximumRowsCountinPage);
        this.numberOfPages++;
    }





}
