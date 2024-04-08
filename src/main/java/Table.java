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
            this.serialize();
            return;
        }
        String clusteringKey = csvConverter.getClusteringKey(this.name);
        Object targetKey = tuple.values.get(clusteringKey);
        Page currPage = null;
        int result=1;
        for(int i = 0; result > 0; i++)
        {
            currPage = Page.deserialize(this.name+"_"+i);

            result =  ((Comparable) targetKey).compareTo((Comparable) currPage.max);
        }
        currPage.insert(tuple);
        this.serialize();




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
        try (FileInputStream fis = new FileInputStream("src/main/resources/tables/" +filename);
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
    public boolean compatibleTypes(Object value, String columnType) {
        switch (columnType.toLowerCase()) {
            case "java.lang.integer":
                return value instanceof Integer;
            case "java.lang.double":
                return value instanceof Double;
            case "java.lang.string":
                return value instanceof String;
        }
        return false;
    }

    public ArrayList<Tuple> searchTable(String columnName, String operator, Object value) throws DBAppException {
        ArrayList<Tuple> results = new ArrayList<>();
        Vector<String[]> columnstuff = Page.readCSV(this.name);
        ArrayList<Tuple> pageResults = new ArrayList<Tuple>();
        String isclusteringkey = "False";
        String columnType=null;
        for (String[] column : columnstuff) {
            if (column[1].equals(columnName)) {
                isclusteringkey = column[3];
                columnType = column[2];
                break;
            }
        }
        if (columnType == null) {
            throw new DBAppException("Column "+ columnName +" not found");
        }
        if (!compatibleTypes(value, columnType)) {
            throw new DBAppException("Datatype of value doesn't match the column datatype: ");
        }
        if (isclusteringkey == "False" || operator == "!=") {
            for (String pagename : tablePages) {
                try {
                    Page page = Page.deserialize(pagename + ".class");// you removed hagat mn hena gt mn salma's incase 3amal moshkela
                    pageResults = new ArrayList<Tuple>();
                    pageResults = page.searchlinearPage(columnName, value, operator);
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
                results.addAll(pageResults);
            }
            return results;
        } else {
            if(operator.equals("=")){
                Comparable<Object> comparableValue = (Comparable<Object>) value;
                int low = 0;
                int high = tablePages.size() - 1;
                while (low <= high) {
                    int mid = (low + high) / 2;
                    String pagename = tablePages.get(mid);
                    try {//getters and setterssss!
                        Page page = Page.deserialize(pagename + ".class");
                        if (comparableValue.compareTo(page.min) < 0) {
                            high = mid - 1; // Search in the lower half
                        } else if (comparableValue.compareTo(page.max) > 0) {
                            low = mid + 1; // Search in the upper half
                        } else {
                            pageResults = page.binarysearchPage(columnName, value, operator);
                            results.addAll(pageResults);
                            return results;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(operator==">"||operator==">=") {
                for (int i = tablePages.size() - 1; i >= 0; i--) {
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i)+".class");
                        int maximum =page.max;
                        if (((Comparable) maximum).compareTo((Comparable) value) > 0) {
                            ArrayList<Tuple> tempp = new ArrayList<>();
                            tempp = page.binarysearchPage(columnName, value, operator);
                            page.serialize();
                            pageResults.addAll(tempp);
                        }else{
                            for(int j= pageResults.size()-1;j>=0;j--){
                                results.add(pageResults.get(i));
                            }
                            return results;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (operator=="<"||operator=="<=") {
                for (int i=0;i< tablePages.size();i++){
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i)+".class");
                        Object minimum =page.min;
                        if (((Comparable) minimum).compareTo((Comparable) value) < 0) {
                            pageResults = new ArrayList<Tuple>();
                            pageResults = page.binarysearchPage(columnName, value, operator);
                            page.serialize();

                            results.addAll(pageResults);
                        }
                        else{
                            return results;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return results;
    }
    public static void main(String[] args){
        Table t1 = new Table("Student");
        t1.serialize();
        Table temp = deserialize("Student/Student.class");
        System.out.println(temp);
        System.out.println(temp.tablePages);
        System.out.println(temp.name);
    }


}