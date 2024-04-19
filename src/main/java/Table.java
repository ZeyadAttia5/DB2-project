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

        //Storing a temporary version of the hashtable of conditions
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            conditionsTemp.put(key, value);
        }

        //Storing references to be deleted from the BP tree
        ArrayList<Ref> toDelete = new ArrayList<>();

        // Looping over the conditions to check if there exists a clustering indexed column or clustering column in the conditions
        for(String column:htblColNameValue.keySet()){
            if(csvConverter.isClusteringKey(name,column)){
                //If the column is the clustering key; binary search is used to find the page it's present in
                Object clusteringKeyValue = htblColNameValue.get(column);
                Page clusteringPage = this.binarySearch(clusteringKeyValue.toString());
                if(clusteringPage==null) {
                    throw new DBAppException("Clustering key value not found in the table");
                }
                else{
                    System.out.println(clusteringPage.name);
                }
                //The clustering key value we are searching for is found in the clusteringPage
                //Check if there's an index on the clustering column to get its reference
                String indexName = csvConverter.getIndexName(name, column);
                if(!indexName.equals("null")){
                    //The clustering column has an index on it; we get the BPTree on that column
                    BPTree b = BPTree.deserialize (name, column);
                    //Get the reference in the BPTree that matches the value in the conditions
                    ArrayList<Ref> reference = b.search((Comparable) htblColNameValue.get(column));
                    if(!reference.isEmpty()) {
                        //There is a matching reference in the BPTree; we will search it to get the corresponding page
                        String pageToGoTo = reference.get(0).getPage();
                        //Check if the reference's page is the same as the clusteringKeyValue's page
                        if (pageToGoTo.equals(clusteringPage.name)) {
                            Page page = Page.deserialize(pageToGoTo);
                            //Go to that page & perform the delete method on it that checks whether to remove it if there are additional conditions in the hashtable
                            Boolean deleted = page.deleteClusteringIndex(reference.get(0), htblColNameValue);
                            page.serialize();
                            b.serialize(name, csvConverter.getIndexName(name, column));
                            //If the reference got deleted it will be added to the ArrayList of references to delete from the tree
                            if (deleted){
                                toDelete.add(reference.get(0)); //but also return so??
                            }
                        }
                    }
                }
                // If the clustering key has no index on it, the deleteTuples method will be called on the conditions hashtable
                else {
                    clusteringPage.deleteTuples(htblColNameValue);
                    clusteringPage.serialize();
                    return; //go to delete from tree
                }
            }
        }

        // Stores the pages that contain references that should be deleted from the BP tree if they match all conditions in the hashtable
        HashSet<String> uniquePages = new HashSet<>();

        // Iterating through the columns to check if there's an index on them
        for (Iterator<String> iterator = htblColNameValue.keySet().iterator(); iterator.hasNext();) {
            String column = iterator.next();
            String indexName = csvConverter.getIndexName(this.name, column);
            // An index was found on a column
            if (!indexName.equals("null")) {
                BPTree b = BPTree.deserialize (this.name, column);
                // Perform a search on the BP tree that gives the corresponding references with a value matching the conditions hashtable
                ArrayList<Ref> references = b.search((Comparable) htblColNameValue.get(column));
                // If there were no previous indices the toDelete gets filled for the first time
                if (toDelete.isEmpty()) {
                    for (Ref ref : references) {
                        toDelete.add(ref);
                    }
                }
                // The toDelete had previous references from previous indices in the conditions so an intersection is being done over the toDelete's previous indices' references & the new index's references
                else{
                    intersection(toDelete,references);
                }
                // The checked indexed column is removed from the conditions hashtable
                iterator.remove();
            }
        }

        // Looping over the references in the toDelete to get their corresponding unique pages
        for(Ref ref:toDelete){
            if(!uniquePages.contains(ref.getPage()))
                uniquePages.add(ref.getPage());
        }


        // Stores the references that will be deleted from the toDelete in case they don't match the rest of the conditions in the hashtable
        ArrayList<Ref> toRemoveFromtoDelete = new ArrayList<>();
         // Checking if there are previously indexed references to check whether they match the rest of the conditions in the hashtable
        if (!uniquePages.isEmpty()) {
            // Looping over the uniquePages of the references
            for (String fileName : uniquePages) {
                try {
                    Page page = Page.deserialize(fileName);
                    // Iterating through the references that are in that page to check if they should be deleted
                    for (Ref ref : toDelete) {
                        if (ref.getPage().equals(fileName)) {
                            // Calling the checkReference method to check if the tuple matches the rest of the conditions to remove it
                            boolean removed = page.checkReference(ref, htblColNameValue);
                            // Checking if the reference didn't match the rest of the conditions to remove it from the toDelete
                            if (!removed) {
                                toRemoveFromtoDelete.add(ref);
                            }
                        }
                    }
                    page.serialize();
                    // Removing all the references that weren't removed as they didn't match all conditions in the hashtable from the toDelete
                    toDelete.removeAll(toRemoveFromtoDelete);
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        // There were no indexed columns in the conditions hashtable; handling all the non-indexed columns
        else {
            for (String fileName : tablePages) {
                try {
                    Page page = Page.deserialize(fileName);
                    // Call the deleteTuples on each page by passing the conditions hashtable to it
                    page.deleteTuples(htblColNameValue);
                    page.serialize();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        //Delete the references that were deleted from the table from the BP tree itself
        for(String column:conditionsTemp.keySet()){
            String indexName = csvConverter.getIndexName(name, column);
            if(!Objects.equals(indexName, "null")){
                for(Ref ref : toDelete){
                    BPTree b = BPTree.deserialize (name, column);
                    b.delete((Comparable) conditionsTemp.get(column), ref);
                    b.serialize(name, column);
                    System.out.println("i am deleted from tree");
                }
            }
        }
    }

    public static <T> ArrayList<T> intersection(ArrayList<T> list1, ArrayList<T> list2) {
        // Store unique elements from list1
        HashSet<T> set = new HashSet<>(list1);

        // Stores the intersection
        ArrayList<T> intersectionList = new ArrayList<>();

        // Iterating through list2 & checking if each element is present in the HashSet
        for (T element : list2) {
            if (set.contains(element)) {
                intersectionList.add(element);
                // Removing the element from the HashSet to avoid duplicates in the intersection
                set.remove(element);
            }
        }

        return intersectionList;
    }

    public Page binarySearch(String clusteringKeyValue) throws IOException, ClassNotFoundException {

        // Find clustering col name
        String clusteringColName = csvConverter.getClusteringKey(this.name);

        // Find the clustering data type
        String dataType = csvConverter.getDataType(this.name, clusteringColName);

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
            Object midPageMin = this.getPageMin(midPageName);

            // Compare clustering key value in midPageMax with clusteringKeyValue
            Comparable<Object> midClusteringKey = (Comparable<Object>) midPageMax;

            int compareResult = midClusteringKey.compareTo(newValue);

            if (this.compareValues(newValue, midPageMax, midPageMin)) {
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
        Page currPage = null;

        for (String pageName : this.pageInfo.keySet()) {
            Object pageMax = this.getPageMax(pageName);
            Object pageMin = this.getPageMin(pageName);
            Object clusteringKeyValueCasted = clusteringKeyValue;

            if (compareValues(clusteringKeyValueCasted, pageMax, pageMin)) {
                currPage = Page.deserialize(pageName);
                break;
            }

        }

        return currPage;
    }

    private boolean compareValues(Object keyValue, Object maxValue, Object minValue) {
        String clusteringKeyColName = csvConverter.getClusteringKey(this.name);
        String dataType = csvConverter.getDataType(this.name, clusteringKeyColName);

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
            Page midPage = Page.deserialize(midPageName);

            // Compare clustering key value in midPage with clusteringKeyValue
            Comparable<Object> midClusteringKey = (Comparable<Object>) midPage.max;

            // Compare midClusteringKey with newValue
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
