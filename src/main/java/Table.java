import java.io.*;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;


public class Table implements Serializable {
    // Map to store the number of pages for each table
    public Vector<String> tablePages;
    public String name;


    public Table(String name) {
        createDirectory(name);
        this.name = name;
        tablePages = new Vector<>();

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

    // Method to get page count for a table
    public int getPageCount() {
        return tablePages.size();
    }

    public void insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {
        String clusteringKey = csvConverter.getClusteringKey(this.name);
        Object targetKey = tuple.values.get(clusteringKey);
        Page currPage = null;
        int result = -1;
        for (int i = 0; result > 0; i++) {
            currPage = Page.deserialize(this.name + "_" + i + ".class");

            if (targetKey instanceof Integer) {
                result = ((Integer) targetKey).compareTo((Integer) currPage.max);
            } else if (targetKey instanceof String) {
                result = ((String) targetKey).compareTo((String) currPage.max);
            } else {
                result = ((Double) targetKey).compareTo((Double) currPage.max);
            }
        }
        currPage.insert(tuple);
    }

    public void serialize() {
        String tableName = name;
        try (FileOutputStream fos = new FileOutputStream(tableName + "/" + name + ".class");
             ObjectOutputStream out = new ObjectOutputStream(fos)) {
            out.writeObject(this);
            System.out.println("saved table successfully at " + tableName + "/" + name + ".class");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateTable(Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException {
        // Find the page where the row with the clustering key value is located
        int pageIndex = findPageIndex(clusteringKeyValue);

        if (pageIndex == -1) {
            // Handle case where row is not found
            System.out.println("Row with clustering key value not found.");
            return;
        }

        // Get the page where the row is located
        String page = tablePages.get(pageIndex);

        // Locate and update the row within the page
        boolean rowUpdated = updateRowInPage(page, clusteringKeyValue, ColNameType);

        if (!rowUpdated) {
            // Handle case where row is not found in the page
            System.out.println("Row not found in the specified page.");
        }
    }

    private int findPageIndex(Object clusteringKeyValue) throws IOException, ClassNotFoundException {
        Page currPage = null;
        Object targetKey = clusteringKeyValue;
        int result = 1;
        for (int i = 0; result > 0; i++) {
            currPage = Page.deserialize(this.name + "_" + i + ".class");

            if (targetKey instanceof Integer) {
                result = ((Integer) targetKey).compareTo((Integer) currPage.max);
            } else if (targetKey instanceof String) {
                result = ((String) targetKey).compareTo((String) currPage.max);
            } else if (targetKey instanceof Double) {
                result = ((Double) targetKey).compareTo((Double) currPage.max);
            } else {
                result = -1;
            }
        }
        currPage.serialize();
        return result;
    }

    private boolean updateRowInPage(String page, Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException {
        Page currPage = Page.deserialize(page);
        String clusteringKeyCol = csvConverter.getClusteringKey(this.name);
        // Iterate through the tuples in the page
        for (Tuple tuple : currPage.tuples) {
            // Check if the tuple's clustering key value matches the specified value
            Object tupleClusteringKeyValue = tuple.getValues().get(clusteringKeyCol);
            if (tupleClusteringKeyValue.equals(clusteringKeyValue)) {
                // Update the columns in the tuple with their new values
                for (String colName : ColNameType.keySet()) {
                    if (tuple.getValues().containsKey(colName)) {
                        String colType = csvConverter.getDataType(this.name, colName);
                        if (colType.equalsIgnoreCase("java.lang.integer")) {
                            Integer newValue = (Integer) ColNameType.get(colName);
                            tuple.getValues().put(colName, newValue);
                        } else if (colType.equalsIgnoreCase("java.lang.string")) {
                            String newValue = (String) ColNameType.get(colName);
                            tuple.getValues().put(colName, newValue);
                        } else if (colType.equalsIgnoreCase("java.lang.double")) {
                            Double newValue = (Double) ColNameType.get(colName);
                            tuple.getValues().put(colName, newValue);
                        }
                    } else {
                        // Handle case where specified column name is not found in the tuple
                        System.out.println("Column not found in the tuple: " + colName);
                    }
                }

                // Serialize the updated page and save it back
                currPage.serialize();
                return true;  // Row updated successfully
            }
        }


        return false;  // Row didn't update successfully
    }

//    public static void main(String[] args){
//        Table t1 = new Table("Student");
//        t1.serialize();
//        Table temp = deserialize("Student/Student.class");
//        System.out.println(temp.tablePages);
//        System.out.println(temp.name);
//
//    }


}