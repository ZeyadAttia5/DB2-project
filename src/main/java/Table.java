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

    }

    public static void createDirectory(String folderPath) {
        // Create a File object representing the directory
        File directory = new File("src/main/resources/tables/" + folderPath);

        // Create the directory if it doesn't exist
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Directory created successfully: " + "src/main/resources/tables/" + folderPath);
            } else {
                System.out.println("Failed to create directory: " + "src/main/resources/tables/" + folderPath);
            }
        } else {
            System.out.println("Directory already exists: " + "src/main/resources/tables/" + folderPath);
        }
    }


    // Method to get page count for a table
    public int getPageCount() {
        return tablePages.size();
    }

    public void insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {
        if (this.tablePages.size()==0)
        {
            Page newPage = new Page(this.name,this.tablePages.size(),csvConverter.getClusteringKey(this.name));
            newPage.insert(tuple);
            tablePages.add(newPage.name);
            return;
        }
        String clusteringKey = csvConverter.getClusteringKey(this.name);
        Object targetKey = tuple.values.get(clusteringKey);
        Page currPage = null;
        int result=-1;
        for(int i = 0; result > 0; i++)
        {
            currPage = Page.deserialize(this.name+"_"+i+".class");

            result =  ((Comparable) targetKey).compareTo((Comparable) currPage.max);
        }
        currPage.insert(tuple);




    }

    public void serialize() {
        String tableName = name;
        try (FileOutputStream fos = new FileOutputStream("src/main/resources/tables/" + tableName + "/" + name + ".class" );
             ObjectOutputStream out = new ObjectOutputStream(fos)) {
            out.writeObject(this);
            System.out.println("saved table successfully at " + "src/main/resources/tables/" + tableName + "/" + name + ".class" );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table deserialize(String filename) {
        Table table = null;
        try (FileInputStream fis = new FileInputStream(filename);
             ObjectInputStream in = new ObjectInputStream(fis)) {
            table = (Table) in.readObject();
            System.out.println("Table deserialized from " + filename);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return table;
    }

    // Method to print all serialized pages for the specified table name
    public static void printTable(String tableName) throws IOException, ClassNotFoundException {
        File directory = new File(tableName); // Use provided table name as directory name
        FilenameFilter filter = (dir, name) -> name.endsWith(".class");
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

    public static void main(String[] args){
        Table t1 = new Table("Student");
        t1.serialize();
        Table temp = deserialize("Student/Student.class");
        System.out.println(temp.tablePages);
        System.out.println(temp.name);

    }


}