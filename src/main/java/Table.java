import java.io.*;
import java.util.ArrayList;
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

        // Inserting a tuple into an empty page
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
        files = Arrays.copyOfRange(files, files.length - this.tablePages.size(), files.length);
        String clusteringKey = csvConverter.getClusteringKey(this.name);
        Object targetKey = tuple.values.get(clusteringKey);
        int maxSize = Page.readConfigFile();
        int result = 1;
        Page currPage = null;

        for (File file : files) {
            String fileName = file.getName();
            Object[] info = this.pageInfo.get(fileName.substring(0, fileName.length() - 6)); //remove .class from the string
            result = ((Comparable) targetKey).compareTo(info[0]);

            if (result == 0) {
                System.out.println("Can't insert duplicate clustering key");
                return;
            }
            if ((int) info[2] < maxSize || result < 0) {
                currPage = Page.deserialize(fileName.substring(0, fileName.length() - 6));
                break;
            }
        }

        if (currPage != null) {
            currPage.insert(tuple);
        } else {
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
    public void updateTable(Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException, DBAppException {
        System.out.println(this);

        // Find the page where the row with the clustering key value is located
        Page page = findPage(clusteringKeyValue);

        if (page == null) {
            // Handle case where row is not found
            throw new DBAppException("Row with clustering key value of " + clusteringKeyValue.toString() + " was not found.");
        }

        // Locate and update the row within the page
        boolean rowUpdated = updateRowInPage(page, clusteringKeyValue, ColNameType);

        if (!rowUpdated) {
            // Handle case where row is not found in the page
            throw new DBAppException("Row with clustering key value of " + clusteringKeyValue.toString() + " was not updated.");
        }
    }

    /**
     * @param clusteringKeyValue
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */

    private Page findPage(Object clusteringKeyValue) throws IOException, ClassNotFoundException {
        Page currPage = null;
        String clusteringKeyColName = csvConverter.getClusteringKey(this.name);
        String clusteringKeyType = csvConverter.getDataType(this.name, clusteringKeyColName);

        for (String pageName : this.pageInfo.keySet()) {
            Object pageMax = this.getPageMax(pageName);
            Object pageMin = this.getPageMin(pageName);
            Object clusteringKeyValueCasted = clusteringKeyValue;

            if (compareValues(clusteringKeyValueCasted, pageMax, pageMin, clusteringKeyType)) {
                currPage = Page.deserialize(pageName);
                break;
            }

        }

        return currPage;
    }

    private boolean compareValues(Object keyValue, Object maxValue, Object minValue, String dataType) {
        if (dataType.equalsIgnoreCase("java.lang.double")) {
            return compareDouble(Double.parseDouble(keyValue.toString()), Double.parseDouble(maxValue.toString()), Double.parseDouble(minValue.toString()));
        } else if (dataType.equalsIgnoreCase("java.lang.String")) {
            return compareString((String) keyValue, (String) maxValue, (String) minValue);
        } else if (dataType.equalsIgnoreCase("java.lang.Integer")) {
            return compareInteger(Integer.parseInt(keyValue.toString()), Integer.parseInt(maxValue.toString()), Integer.parseInt(minValue.toString()));
        }
        return false;
    }

    private boolean compareDouble(Double keyValue, Double maxValue, Double minValue) {
        return (keyValue < maxValue && keyValue > minValue) || keyValue.equals(minValue) || keyValue.equals(maxValue);
    }

    private boolean compareString(String keyValue, String maxValue, String minValue) {
        return (keyValue.compareTo(maxValue) < 0 && keyValue.compareTo(minValue) > 0) || keyValue.equals(minValue) || keyValue.equals(maxValue);
    }

    private boolean compareInteger(Integer keyValue, Integer maxValue, Integer minValue) {
        return (keyValue < maxValue && keyValue > minValue) || keyValue.equals(minValue) || keyValue.equals(maxValue);
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

        Object clusteringKeyValueCasted = null;
        ArrayList<Ref> searchSet = new ArrayList<Ref>();
        if (clusteringDataType.equalsIgnoreCase("java.lang.integer")) {
            clusteringKeyValueCasted = Integer.parseInt((String) clusteringKeyValue);
            searchSet = tree.search((Integer) clusteringKeyValueCasted);
        } else if (clusteringDataType.equalsIgnoreCase("java.lang.string")) {
            clusteringKeyValueCasted = clusteringKeyValue.toString();
            searchSet = tree.search((String) clusteringKeyValueCasted);
        } else if (clusteringDataType.equalsIgnoreCase("java.lang.double")) {
            clusteringKeyValueCasted = Double.parseDouble((String) clusteringKeyValue);
            searchSet = tree.search((Double) clusteringKeyValueCasted);
        }


        // find the tuple from the set of pages
        Page page = this.findPageIndexed(searchSet, clusteringKeyValue, clusteringDataType);

        if (page == null) {
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

    private Page findPageIndexed(ArrayList<Ref> searchSet, Object clusteringKeyValue, String clusteringDataType) {

        Object clusteringKeyValueCasted = null;
        if (clusteringDataType.equalsIgnoreCase("java.lang.integer")) {
            clusteringKeyValueCasted = Integer.parseInt((String) clusteringKeyValue);
        } else if (clusteringDataType.equalsIgnoreCase("java.lang.string")) {
            clusteringKeyValueCasted = clusteringKeyValue.toString();
        } else if (clusteringDataType.equalsIgnoreCase("java.lang.double")) {
            clusteringKeyValueCasted = Double.parseDouble((String) clusteringKeyValue);
        }

        Page page = null;
        for (Ref ref : searchSet) {

        }
        return page;
    }


    public Page binarySearch(String clusteringKeyValue, String dataType) throws IOException, ClassNotFoundException {

        Object newValue = null;
        if (dataType.equalsIgnoreCase("java.lang.integer")) {
            newValue = Integer.parseInt(clusteringKeyValue);
        } else if (dataType.equalsIgnoreCase("java.lang.string")) {
            newValue = clusteringKeyValue;
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
            Object midPageMax = this.getPageMax(midPageName);

            // Compare clustering key value in midPageMax with clusteringKeyValue
            Comparable<Object> midClusteringKey = (Comparable<Object>) midPageMax;

            int compareResult = midClusteringKey.compareTo(newValue);

            if (compareResult == 0) {
                // Found exact match, return midPageMax
                return Page.deserialize(midPageName);

            } else if (compareResult < 0) {
                // If midPageMax's clustering key is less than clusteringKeyValue, search right half
                low = mid + 1;
            } else {
                // If midPageMax's clustering key is greater than clusteringKeyValue, search left half
                high = mid - 1;
            }
        }

        // If not found, return null
        return null;
    }

    @Override
    public String toString() {
        String result = "";
        for (String page_name : this.tablePages) {
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
        String columnType = csvConverter.getColumnType(this.name, columnName);

        if (columnType == null) {
            throw new DBAppException("Column " + columnName + " not found");
        }
        if (!compatibleTypes(value, columnType)) {
            throw new DBAppException("Datatype of value doesn't match the column datatype: ");
        }

        // Linear searching
        if (isclusteringkey.equals("False") || operator.equals("!=")) {
            for (String pagename : tablePages) {
                try {
                    Page page = Page.deserialize(pagename);
                    pageResults = new ArrayList<Tuple>();
                    pageResults = page.searchlinearPage(columnName, value, operator);
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
                results.addAll(pageResults);
            }
            return results;
        } else {
            if (operator.equals("=")) {
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
            if (operator.equals(">") || operator.equals(">=")) {
                for (int i = tablePages.size() - 1; i >= 0; i--) {
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i) + ".class");
                        Object maximum = page.max;
                        if (((Comparable) maximum).compareTo(value) > 0) {
                            ArrayList<Tuple> tempp = new ArrayList<>();
                            tempp = page.binarysearchPage(columnName, value, operator);
                            page.serialize();
                            pageResults.addAll(tempp);
                        } else {
//                            for(int j= pageResults.size()-1;j>=0;j--){
//                                results.add(pageResults.get(i));
//                            }
                            return results;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (operator.equals("<") || operator.equals("<=")) {
                for (int i = 0; i < tablePages.size(); i++) {
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i) + ".class");
                        Object minimum = page.min;
                        if (((Comparable) minimum).compareTo(value) < 0) {
                            pageResults = new ArrayList<Tuple>();
                            pageResults = page.binarysearchPage(columnName, value, operator);
                            page.serialize();

                            results.addAll(pageResults);
                        } else {
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

    public Hashtable<String, Object[]> getPageInfo() {
        return this.pageInfo;
    }

    public Object getPageMax(String pageName) {
        return this.pageInfo.get(pageName)[0];
    }

    public Object getPageMin(String pageName) {
        return this.pageInfo.get(pageName)[1];
    }

    public Object getPageSize(String pageName) {
        return this.pageInfo.get(pageName)[2];
    }
}


//    Old findPage... Keep it just in case we need it
//    private Page findPage(Object clusteringKeyValue) throws IOException, ClassNotFoundException {
//        int result = 1;
//        Page currPage = null;
//
//
//        String clusteringKeyColName = csvConverter.getClusteringKey(this.name);
//        String clusteringKeyType = csvConverter.getDataType(this.name, clusteringKeyColName);
//
//        if (clusteringKeyType.equalsIgnoreCase("java.lang.double")) {
//
//            for (String pageName : this.pageInfo.keySet()) {
//                Double pageMax = Double.parseDouble((String) this.getPageMax(pageName));
//                Double pageMin = Double.parseDouble((String) this.getPageMin(pageName));
//                Double clusteringKeyValueCasted = Double.parseDouble((String) clusteringKeyValue);
////                result = Double.compare(pageMax, clusteringKeyValueCasted);
//
//                // if the clusteringKeyValue is between pageMax and pageMin or equal to either, then page is found
//                if ((clusteringKeyValueCasted < pageMax && clusteringKeyValueCasted > pageMin) || clusteringKeyValueCasted == pageMin || clusteringKeyValueCasted == pageMax) {
//                    currPage = Page.deserialize(pageName);
//                    break;
//                }
//            }
//        } else if (clusteringKeyType.equalsIgnoreCase("java.lang.string")) {
//            for (String pageName : this.pageInfo.keySet()) {
//                String pageMax = (String) this.getPageMax(pageName);
//                String pageMin = (String) this.getPageMin(pageName);
//                String clusteringKeyValueCasted = (String) clusteringKeyValue;
//                int isMaxGreater = clusteringKeyValueCasted.compareTo(pageMax);
//                int isMinGreater = clusteringKeyValueCasted.compareTo(pageMin);
//
//                // case 1: the clusteringKeyValue is between both min and max
//                // case 2: the clusteringKeyValue is equal to the maximum
//                // case 3: the clusteringKeyValue is equal to the minimum
//                if ((isMinGreater > 0 && isMaxGreater < 0) || isMinGreater == 0 || isMaxGreater == 0) {
//                    currPage = Page.deserialize(pageName);
//                    break;
//                }
//            }
//        } else if (clusteringKeyType.equalsIgnoreCase("java.lang.Integer")) {
//            for (String pageName : this.pageInfo.keySet()) {
//                Integer pageMax = Integer.parseInt(this.getPageMax(pageName).toString());
//                Integer pageMin = Integer.parseInt(this.getPageMin(pageName).toString());
//                Integer clusteringKeyValueCasted = Integer.parseInt((String) clusteringKeyValue);
//
//                // if the clusteringKeyValue is between pageMax and pageMin or equal to either, then page is found
//                if ((clusteringKeyValueCasted < pageMax && clusteringKeyValueCasted > pageMin) || clusteringKeyValueCasted == pageMin || clusteringKeyValueCasted == pageMax) {
//                    currPage = Page.deserialize(pageName);
//                    break;
//                }
//            }
//        }
//        return currPage;
//
//    }