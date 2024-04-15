import java.io.*;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;



public class Table implements Serializable {
    // Map to store the number of pages for each table
    public Vector<String> tablePages;
    public String name;
    public Hashtable<String, Object[]> pageInfo; // Object [] = [max,min,pageSize]


    public Table(String name) {
        createDirectory(name);
        this.name = name;
        tablePages = new Vector<>();
        pageInfo = new Hashtable<>();

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

    public static Table deserialize(String filename) {
        Table table = null;
        try (FileInputStream fis = new FileInputStream("src/main/resources/tables/" + filename + "/" + filename + ".class"); ObjectInputStream in = new ObjectInputStream(fis)) {
            table = (Table) in.readObject();
            System.out.println("Table deserialized from " + "src/main/resources/tables/" + filename + "/" + filename + ".class");
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

        if (this.tablePages.size() == 0) {
            Page newPage = new Page(this.name, this.tablePages.size(), csvConverter.getClusteringKey(this.name));
            newPage.insert(tuple);
            tablePages.add(newPage.name);
            pageInfo.put(newPage.name, new Object[]{newPage.max, newPage.min, newPage.tuples.size()});
            this.serialize();
            return;
        }

        File pageFolder = new File("src/main/resources/tables/" + this.name);
        File[] files = Page.sortFiles(pageFolder.listFiles());
        files = Arrays.copyOfRange(files, 1,  files.length);
        String clusteringKey = csvConverter.getClusteringKey(this.name);
        Object targetKey = tuple.values.get(clusteringKey);
        int maxSize = Page.readConfigFile();
        int result = 1;
        Page currPage = null;

        for(File file : files)
        {
            String fileName = file.getName();
            Object[] info = this.pageInfo.get(fileName.substring(0, fileName.length()-6)); //remove .class from the string
            result = ((Comparable) targetKey).compareTo(info[0]);

            if(result==0)
            {
                System.out.println("Can't insert duplicate clustering key");
                return;
            }
            if((int) info[2] < maxSize || result < 0)
            {
                currPage = Page.deserialize(fileName.substring(0, fileName.length()-6));
                break;
            }
        }

        if(currPage != null)
        {
            currPage.insert(tuple);
        }
        else
        {
            Page newPage = new Page(this.name, this.tablePages.size(), csvConverter.getClusteringKey(this.name));
            newPage.insert(tuple);
            pageInfo.put(newPage.name, new Object[]{newPage.max, newPage.min, newPage.tuples.size()});
        }



    }

    /**
     * @param clusteringKeyValue
     * @param ColNameType
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void updateTable(Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException {
        System.out.println(this);

        // Find the page where the row with the clustering key value is located
        Page page = findPage(clusteringKeyValue);

        if (page == null) {
            // Handle case where row is not found
            System.out.println("Row with clustering key value not found.");
            return;
        }

        // Locate and update the row within the page
        boolean rowUpdated = updateRowInPage(page, clusteringKeyValue, ColNameType);

        if (!rowUpdated) {
            // Handle case where row is not found in the page
            System.out.println("Row not found in the specified page.");
        }
    }

    /**
     * @param clusteringKeyValue
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Page findPage(Object clusteringKeyValue) throws IOException, ClassNotFoundException {
        int result = 1;
        Page currPage = null;


        String clusteringKeyColName = csvConverter.getClusteringKey(this.name);
//        System.out.println("clusteringKeyColName: "+ clusteringKeyColName);
        String clusteringKeyType = csvConverter.getDataType(this.name, clusteringKeyColName);
//        System.out.println("clusteringKeyType: "+ clusteringKeyType);
        if (clusteringKeyType.equalsIgnoreCase("java.lang.double")) {

            for (int i = 0; i < tablePages.size() && result != -1 && result != 0; i++) {

                currPage = Page.deserialize(this.name + "_" + i);
                result = Double.compare(Double.parseDouble((String) clusteringKeyValue), (Double) currPage.max);
            }
        } else if (clusteringKeyType.equalsIgnoreCase("java.lang.string")) {
            //this means that we have a string at hand
            for (int i = 0; i < tablePages.size() - 1 && result != -1 && result != 0; i++) {
                currPage = Page.deserialize(this.name + "_" + i);
                result = ((String) clusteringKeyValue).compareTo((String) currPage.max);
            }
        } else {
//            System.out.println("Entered Integer :D");
            for (int i = 0; i < tablePages.size() && result != -1 && result != 0; i++) {
                currPage = Page.deserialize(this.name + "_" + i);
                result = Integer.compare(Integer.parseInt((String) clusteringKeyValue), (Integer) currPage.max);
            }
        }
        return currPage;

    }


    /**
     * @param currPage
     * @param clusteringKeyValue
     * @param ColNameType
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private boolean updateRowInPage(Page currPage, Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException {
        String clusteringKeyCol = csvConverter.getClusteringKey(this.name);

        // Iterate through the tuples in the page
        for (Tuple tuple : currPage.tuples) {
            // Check if the tuple's clustering key value matches the specified value
            Object tupleClusteringKeyValue = tuple.getValues().get(clusteringKeyCol);
            if ((tupleClusteringKeyValue.toString()).equals(clusteringKeyValue.toString())) {
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

    public void serialize() {
        String tableName = name;
        try (FileOutputStream fos = new FileOutputStream("src/main/resources/tables/" + tableName + "/" + name + ".class"); ObjectOutputStream out = new ObjectOutputStream(fos)) {
            out.writeObject(this);
            System.out.println("saved table successfully at " + "src/main/resources/tables/" + tableName + "/" + name + ".class");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Updates a tuple in a table that has an indexed column
     *
     * @param indexedColName
     * @param clusteringKeyValue
     * @param htblColNameType    * @throws IOException
     * @throws ClassNotFoundException
     * @throws DBAppException
     */
    public void updateIndexedTable(String indexedColName, Object clusteringKeyValue, Hashtable<String, Object> htblColNameType) throws IOException, ClassNotFoundException, DBAppException {
        /*
            update can be done in-place since the clustering key value is NEVER updated;
            therefore, there is no need to insert then update
         */

        // deserialize B+ tree
        BPTree tree = BPTree.deserialize(this.name, indexedColName);

        // Find clustering col name
        String clusteringColName = csvConverter.getClusteringKey(this.name);

        // Find the clustering data type
        String clusteringDataType = csvConverter.getDataType(this.name, clusteringColName);

        // Find the Page
        Page page = this.binarySearch(clusteringKeyValue.toString(), clusteringDataType);

        if(page == null){
            throw new DBAppException("tuple not found :)");
        }

        // initialize old Ref
        int indexInPage = page.binarySearchPage(clusteringKeyValue.toString(), clusteringDataType);
        if (indexInPage == -1) {
            // entry not found
            System.out.println(clusteringKeyValue + " was not found in " + this.name);
            return;
        }

        // Initializes refs
        Ref oldRef = new Ref(page.name, indexInPage);
        Ref newRef = new Ref(page.name, indexInPage);

        // update in place
        boolean isUpdated = page.updateTuple(oldRef, htblColNameType);
        if (!isUpdated) {
            System.out.println("updateIndexedTable failed");
        }

        // update B+ tree
        tree.update((Comparable) clusteringKeyValue, (Comparable) clusteringKeyValue, oldRef, newRef);
        tree.serialize(this.name, "B+ Tree");

    }


    public Page binarySearch(String clusteringKeyValue, String dataType) throws IOException, ClassNotFoundException {

        Object newValue = null;
        if (dataType.equalsIgnoreCase("java.lang.integer")) {
            newValue = Integer.parseInt(clusteringKeyValue);
        } else if (dataType.equalsIgnoreCase("java.lang.string")) {
            newValue = (String) clusteringKeyValue;
        } else if (dataType.equalsIgnoreCase("java.lang.double")) {
            newValue = Double.parseDouble(clusteringKeyValue);
        }


        // Initialize variables for binary search
        int low = 0;
        int high = tablePages.size() - 1;

        // Binary search loop
        while (low <= high) {
            int mid = (low + high) / 2;
            String midPageName = tablePages.get(mid);
            Page midPage = Page.deserialize(midPageName);

            // Compare clustering key value in midPage with clusteringKeyValue
            Comparable<Object> midClusteringKey = (Comparable<Object>) midPage.max;

            int compareResult = midClusteringKey.compareTo(newValue);

            if (compareResult == 0) {
                // Found exact match, return midPage
                return midPage;
            } else if (compareResult < 0) {
                // If midPage's clustering key is less than clusteringKeyValue, search right half
                low = mid + 1;
            } else {
                // If midPage's clustering key is greater than clusteringKeyValue, search left half
                high = mid - 1;
            }
        }

        // If not found, return null
        return null;
    }

    @Override
    public String toString(){
        String result = "";
            for (String page_name: this.tablePages){
                Page page = null;
                try {
                    page = Page.deserialize(page_name);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                result += page.toString();
            }

        return result;
    }


}