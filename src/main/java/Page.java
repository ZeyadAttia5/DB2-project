import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {
    public String  name;
    public int maxSize;
    public Vector<Tuple> tuples;


    public Page(String name){ //initializes an empty page   //name refers to the table name, and this.name updates it to the naming convention
        int count = Table.getPageCount(name);
        this.name = name + "_" + count;
        this.tuples = new Vector<Tuple>();
        this.maxSize = readConfigFile();
        updateTablePages(name,this.name);
    }


    public void insert(Tuple tuple) throws DBAppException {

        if(!isFull()) {
            tuples.add(tuple);
            serialize();
        }
        else {
            throw new DBAppException("page full");
        }
    }

    // Method to serialize the Page object
    public void serialize() {
        String tableName = getTname(name);
        try (FileOutputStream fos = new FileOutputStream(tableName + "/" + name + ".ser" );
             ObjectOutputStream out = new ObjectOutputStream(fos)) {
            out.writeObject(this);
            System.out.println("saved page successfully at " + tableName + "/" + name + ".ser" );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Page deserialize(String filename) {
        Page page = null;
        try (FileInputStream fis = new FileInputStream(filename);
             ObjectInputStream in = new ObjectInputStream(fis)) {
            page = (Page) in.readObject();
            System.out.println("Page deserialized from " + filename);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return page;
    }

//get maxSize from config file
    public int readConfigFile(){
        Properties properties = new Properties();
        String fileName = "resources/DBApp.config";
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            properties.load(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String maxSizeStr = properties.getProperty("MaximumRowsCountinPage");
            if (maxSizeStr != null) {
               return maxSize = Integer.parseInt(maxSizeStr);
            } else {

               return maxSize = 200; // Default value
            }
    }

    public static void updateTablePages(String tableName, String pageName) {
        // Get the list of page names for the specified table
        ArrayList<String> pages = Table.tablePages.getOrDefault(tableName, new ArrayList<>());

        // Add the page name to the list
        pages.add(pageName);

        // Update the tablePages map with the updated list of page names
        Table.tablePages.put(tableName, pages);
    }

    public Boolean isFull(){
        return tuples.size() == maxSize;
    }

    public String getTname(String s){
        int indexOfUnderscore = s.indexOf('_');
        return s.substring(0, indexOfUnderscore);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();


        // Iterate over tuples and append their string representations with commas
        for (int i = 0; i < tuples.size(); i++) {
            sb.append(tuples.get(i));
            if (i < tuples.size() - 1) {
                sb.append("\n");
            }
        }

        return sb.toString();
    }

//    public static void main(String[] args) throws DBAppException {
//        Table Student = new Table ("student");
//        Tuple t1 = new Tuple(1,21,"ahmed");
//        Student.insert(t1);
//        System.out.println("table pages  "+Table.tablePages);
//        System.out.println("--------------------------------------------------------------");
//
//        Tuple t2 = new Tuple(1,21,"omar");
//        Student.insert(t2);
//        Table.printTable("Student");
////        System.out.println(Table.tablePages);
////        System.out.println(Table.tablePageCount);
//
//
////      Tuple t3 = new Tuple(1,21,"nora");
////      Tuple t4= new Tuple(1,21,"mona");
////      Tuple t5= new Tuple(1,21,"mohamed");
////      Tuple t6= new Tuple(1,21,"emad");
////      Tuple t7= new Tuple(1,21,"JD");
////
////
////
////      Page student0 = deserialize("student/student_0.ser");
////      student0.insert(t1);
////      student0.insert(t2);
////
////      student0.insert(t2);
////
////      System.out.println(student0);
////      Table.printTable("student");
//
//
//
//
//
//
//
//
//
//
//    }


}
