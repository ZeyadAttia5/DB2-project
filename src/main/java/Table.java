import java.util.ArrayList;
import java.io.File;

public class Table {
    ArrayList<String> availableTables = new ArrayList<>(); //not sure

    ArrayList<Page> pages = new ArrayList<>(); //not sure

    public String name;

    public static int count = 0;


    public Table(String name){
        createDirectory(name);
        this.name = name;
        Page page = new Page(name);
        pages.add(page);
        availableTables.add(name);
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


    public static void main(String[] args){

        Table student = new Table("student");
    }


}
