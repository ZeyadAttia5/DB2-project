import java.io.*;
import java.util.*;


public class Table implements Serializable {
    // Map to store the number of pages for each table
    public Vector<String> tablePages;
    public String name;


    public Table(String name){
        createDirectory(name);
        this.name = name;
        tablePages = new Vector<>();
        serialize();

    }

    public static void createDirectory(String folderPath) {
        // Create a File object representing the directory
        File directory = new File("src/main/resources/tables/" + folderPath);

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
    public int getPageCount() {
       return tablePages.size();
    }
//    public void insert(Tuple tuple) throws DBAppException {
//         if (getPageCount() == 0) {
//            Page page = new Page(name,this.tablePages.size());
//            page.insert(tuple);
//            tablePages.add(page.name);
//        } else {
//            Vector<String> pageFileNames = this.tablePages;
//            int lastIndex = pageFileNames.size() - 1;
//            int currentIndex = 0;
//            for (String fileName : pageFileNames) {//still not handled when full
//                Page page = Page.deserialize(name + "/" + fileName + ".class");
//                if(!page.isFull()) {
//                    page.insert(tuple);
//                    break;
//                }
//                else if (currentIndex == lastIndex){
//                    Page nPage = new Page(name, this.tablePages.size());
//                    nPage.insert(tuple);
//                }
//                currentIndex++;
//            }
//
//
//        }
//    }



    public void serialize() {
        String tableName = name;
        try (FileOutputStream fos = new FileOutputStream("src/main/resources/tables/" + tableName + "/" + name + ".class" );
             ObjectOutputStream out = new ObjectOutputStream(fos)) {
                out.writeObject(this);
                System.out.println("saved table successfully at  " + "src/main/resources/tables/" + tableName + "/" + name + ".class" );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table deserialize(String filename) {
        Table table = null;
        try (FileInputStream fis = new FileInputStream("src/main/resources/tables/" + filename);
             ObjectInputStream in = new ObjectInputStream(fis)) {
            table = (Table) in.readObject();
            System.out.println("Table deserialized from " + "src/main/resources/tables/" + filename);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return table;
    }

    // Method to print all serialized pages for the specified table name
//    public static void printTable(String tableName) {
//        File directory = new File(tableName); // Use provided table name as directory name
//        FilenameFilter filter = (dir, name) -> name.endsWith(".class");
//        File[] serializedPages = directory.listFiles(filter);
//        System.out.println(Arrays.toString(serializedPages));
//
//        if (serializedPages != null && serializedPages.length > 0) {
//            System.out.println("pages for table " + tableName + ":");
//            for (File pageFile : serializedPages) {
//                System.out.println("File name: " + pageFile.getName());
//                Page page = Page.deserialize(tableName + "/" + pageFile.getName());
//                System.out.println(page);
//            }
//        } else {
//            System.out.println("No serialized pages found for table " + tableName);
//        }
//    }

//    public static void main(String[] args){
//        Table t1 = new Table("Student");
//        t1.serialize();
//        Table temp = deserialize("Student/Student.class");
//        System.out.println(temp.tablePages);
//        System.out.println(temp.name);
//
//    }


}
