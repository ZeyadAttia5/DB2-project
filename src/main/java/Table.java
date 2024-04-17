import java.io.*;
import java.util.*;


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
        files = Arrays.copyOfRange(files, files.length-this.tablePages.size(),  files.length);
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
    public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Hashtable<String, Object> conditionsTemp = new Hashtable<>();
        Enumeration<String> keys = htblColNameValue.keys();

        // Storing a temporary version of the conditions (ht)
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            conditionsTemp.put(key, value);
        }

        // Deleting one row in the case of a clustering indexed key being present
        for(String column:htblColNameValue.keySet()){
            if(csvConverter.isClusteringKey(name,column)){
                String indexName = csvConverter.getIndexName(name, column);
                if(!indexName.equals("null")){
                    System.out.println("clustering index column");
                    BPTree b = BPTree.deserialize (name, column);
                    System.out.println(htblColNameValue.get(column));
                    ArrayList<Ref> reference = b.search((Comparable) htblColNameValue.get(column));
                    if (reference != null && !reference.isEmpty()) {
                        System.out.println(reference.get(0));
                        String pageToGoTo = reference.get(0).getPage();
                        System.out.println(pageToGoTo);
                        Page page = Page.deserialize(pageToGoTo);
                        page.deleteClusteringIndex(reference.get(0),htblColNameValue);
                        page.serialize();
                        b.serialize(name, csvConverter.getIndexName(name, column));
                        return;
                    } else {
                        System.out.println("no matching tuple in index");
                        b = BPTree.deserialize (name, column);
                        b.delete((Comparable) conditionsTemp.get(column), reference.get(0));
                        b.serialize(name, column);
                        break;
                    }
                }
            }
        }

        ArrayList<Ref> toDelete = new ArrayList<>();
        HashSet<String> uniquePages = new HashSet<>();

        // We will iterate through the columns to check if there's an index on them
        for (Iterator<String> iterator = htblColNameValue.keySet().iterator(); iterator.hasNext();) {
            String column = iterator.next();
            String indexName = csvConverter.getIndexName(this.name, column);
            System.out.println(indexName);
            // If an index was found on a column it should be added to the array of references to delete
            if (!indexName.equals("null")) {
                BPTree b = BPTree.deserialize (this.name, column);
//                System.out.println(htblColNameValue.get(column));
                ArrayList<Ref> references = b.search((Comparable) htblColNameValue.get(column));
                if (toDelete.isEmpty()) {
                    System.out.println("toDelete is empty im entering for the first time");
                    for (Ref ref : references) {
                        toDelete.add(ref);
                    }
                }
                else{
                    System.out.println("toDelete has other references");
                    intersection(toDelete,references);
                }
//                System.out.println(htblColNameValue.size());
                //remove the indexed column from the conditions
                iterator.remove();
//                System.out.println(htblColNameValue.size());
            }
            else{
                System.out.println("i didnt find an index");
            }
        }
//        System.out.println(toDelete.size());
        for(Ref ref:toDelete){
            if(!uniquePages.contains(ref.getPage()))
                uniquePages.add(ref.getPage());
        }
//        System.out.println(uniquePages.size());

//        System.out.println(htblColNameValue.isEmpty());

        ArrayList<Ref> toRemoveFromtoDelete = new ArrayList<>();
        // Checking if there are columns in the conditions hashtable where an index doesn't exist
        //if (!htblColNameValue.isEmpty()) {
            //checking if there are previously indexed references to check
            if (!uniquePages.isEmpty()) {
                for (String fileName : uniquePages) {
                    try {
                        Page page = Page.deserialize(fileName);
                        //we will iterate through the references that should be deleted if they exist
                        for (Ref ref : toDelete) {
                            if (ref.getPage().equals(fileName))
                            {
                                //call the page checkIndexedTuples method to check if the tuple matches the rest of the conditions by accessing the number of the tuple specified in the reference
                                boolean removed = page.checkReference(ref, htblColNameValue);
//                                System.out.println(removed);
                                toRemoveFromtoDelete.add(ref);
                            }
                        }
                        page.serialize();
                        toDelete.removeAll(toRemoveFromtoDelete);
//                        for(Ref ref : toDelete){
//                            if(ref.getPage().equals(fileName)){
//                                deleteReferencedTuples(ref,htblColNameValue);
//                            }
//                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            //there are no indexed columns
            } else {
                for (String fileName : tablePages) {
                    try {
                        Page page = Page.deserialize(fileName);
                        page.deleteTuples(htblColNameValue);
                        page.serialize();
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }

        //delete from the tree
        for(String column:conditionsTemp.keySet()){
            String indexName = csvConverter.getIndexName(name, column);
            if(!Objects.equals(indexName, "null")){
                for(Ref ref : toDelete){
                    BPTree b = BPTree.deserialize (name, column);
                    b.delete((Comparable) conditionsTemp.get(column), ref);
                    b.serialize(name, column);
                }
            }
        }
    }

    public static <T> ArrayList<T> intersection(ArrayList<T> list1, ArrayList<T> list2) {
        // Create a HashSet to store unique elements from list1
        HashSet<T> set = new HashSet<>(list1);

        // Create a result ArrayList to store the intersection
        ArrayList<T> intersectionList = new ArrayList<>();

        // Iterate through list2 and check if each element is present in the HashSet
        for (T element : list2) {
            if (set.contains(element)) {
                intersectionList.add(element);
                // Remove the element from the HashSet to avoid duplicates in the intersection
                set.remove(element);
            }
        }

        return intersectionList;
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
                        Object maximum =page.max;
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

}
