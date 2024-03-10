import java.io.FilenameFilter;
import java.util.ArrayList;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class Table {
    // Map to store the number of pages for each table
    public static HashMap<String, ArrayList<String>> tablePages = new HashMap<>();
    public String name;

    public Table(String name){
        createDirectory(name);
        this.name = name;
        tablePages.put(name,new ArrayList<>());
    }

    public static void createDirectory(String folderPath) {
        // Create a File object representing the directory
        File directory = new File(folderPath);

        // Create the directory if it doesn't exist
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Directory created successfully: " + folderPath);
            } else {
                System.out.println("Failed to create directory: " + folderPath);
            }
        } else {
            System.out.println("Directory already exists: " + folderPath);
        }
    }



    // Method to get page count for a table
    public static int getPageCount(String tableName) {
        if(tablePages.get(tableName)==null)
            return -1;
        else
            return tablePages.get(tableName).size();
    }
    public void insert(Tuple tuple) throws DBAppException {
         if (getPageCount(name) == 0) {
            Page page = new Page(name);
            page.insert(tuple);
        } else {
            ArrayList<String> pageFileNames = tablePages.get(name);
            int lastIndex = pageFileNames.size() - 1;
            int currentIndex = 0;
            for (String fileName : pageFileNames) {//still not handled when full
                Page page = Page.deserialize(name + "/" + fileName + ".ser");
                if(!page.isFull()) {
                    page.insert(tuple);
                    break;
                }
                else if (currentIndex == lastIndex){
                    Page nPage = new Page(name);
                    nPage.insert(tuple);
                }
                currentIndex++;
            }


        }
    }

    // Method to print all serialized pages for the specified table name
    public static void printTable(String tableName) {
        File directory = new File(tableName); // Use provided table name as directory name
        FilenameFilter filter = (dir, name) -> name.endsWith(".ser");
        File[] serializedPages = directory.listFiles(filter);
        System.out.println(Arrays.toString(serializedPages));

        if (serializedPages != null && serializedPages.length > 0) {
            System.out.println("pages for table " + tableName + ":");
            for (File pageFile : serializedPages) {
                System.out.println("File name: " + pageFile.getName());
                Page page = Page.deserialize(tableName + "/" + pageFile.getName());
                System.out.println(page);
            }
        } else {
            System.out.println("No serialized pages found for table " + tableName);
        }
    }
}
