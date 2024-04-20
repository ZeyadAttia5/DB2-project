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

    // =======================================================================================================================================
    //  Table configuration, serialisation and deserialisation
    // =======================================================================================================================================

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

    public void serialize() {
        String tableName = name;
        try (FileOutputStream fos = new FileOutputStream("src/main/resources/tables/" + tableName + "/" + name + ".class"); ObjectOutputStream out = new ObjectOutputStream(fos)) {
            out.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table deserialize(String filename) {
        Table table = null;
        try (FileInputStream fis = new FileInputStream("src/main/resources/tables/" + filename + "/" + filename + ".class"); ObjectInputStream in = new ObjectInputStream(fis)) {
            table = (Table) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return table;
    }


    // =======================================================================================================================================
    //  Table insertion
    // =======================================================================================================================================

    public void insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {

        for(String key : tuple.values.keySet())
        {
            Object para = tuple.values.get(key);
            String paraType = para.getClass().getName();
            String requiredType = csvConverter.getDataType(this.name,key);
            if(!paraType.equals(requiredType))
            {
                throw new DBAppException("invalid tuple datatype");
            }
        }

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


    // =======================================================================================================================================
    //  Table deletion
    // =======================================================================================================================================

    public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Hashtable<String, Object> conditionsTemp = new Hashtable<>();
        Enumeration<String> keys = htblColNameValue.keys();

        // Storing a temporary version of the hashtable of conditions
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Object value = htblColNameValue.get(key);
            conditionsTemp.put(key, value);
        }

        //Storing references to be deleted from the BP tree
        ArrayList<Ref> toDelete = new ArrayList<>();
        Ref CIref;

        // Looping over the conditions to check if there exists a clustering indexed column or clustering column in the conditions
        for (String column : htblColNameValue.keySet()) {
            if (csvConverter.isClusteringKey(name, column)) {
                //If the column is the clustering key; binary search is used to find the page it's present in
                Object clusteringKeyValue = htblColNameValue.get(column);
                Page clusteringPage = this.binarySearch(clusteringKeyValue.toString());
                if (clusteringPage == null) {
                    System.out.println("Clustering key value not found in the table");
                }
                //The clustering key value we are searching for is found in the clusteringPage
                //Check if there's an index on the clustering column to get its reference
                String indexName = csvConverter.getIndexName(name, column);
                if (!indexName.equals("null")) {
                    BPTree b = BPTree.deserialize(name, column);
                    ArrayList<Ref> reference = b.search((Comparable) htblColNameValue.get(column));
                    if (!reference.isEmpty()) {
                        String pageToGoTo = reference.get(0).getPage();
                        if (pageToGoTo.equals(clusteringPage.name)) {
                            Page page = Page.deserialize(pageToGoTo);
                            Boolean deleted = page.deleteClusteringIndex(reference.get(0), htblColNameValue);
                            page.serialize();
                            if (deleted) {
                                toDelete.add(reference.get(0));
                            }
                        }
                    }
                }
                // If the clustering key has no index on it, the deleteTuples method will be called on the conditions hashtable
                else {
                    ArrayList<Ref> reference = clusteringPage.deleteTuples(htblColNameValue);
                    toDelete.add(reference.get(0));
                    clusteringPage.serialize();
                }
            }
        }
        // Store the reference that should be deleted in the CIref
        if (!toDelete.isEmpty()) {
            CIref = toDelete.get(0);
            toDelete.remove(0);
        } else
            CIref = null;

        // Stores the pages that contain references that should be deleted from the BP tree if they match all conditions in the hashtable
        HashSet<String> uniquePages = new HashSet<>();

        // Iterating through the columns to check if there's an index on them
        for (Iterator<String> iterator = htblColNameValue.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String indexName = csvConverter.getIndexName(this.name, column);
            if (!indexName.equals("null")) {
                BPTree b = BPTree.deserialize(this.name, column);
                ArrayList<Ref> references = b.search((Comparable) htblColNameValue.get(column));
                if (references == null) {
                    System.out.println("Conditions not matched");
                    return;
                }
                intersection(toDelete, references);
                iterator.remove();
            }
        }

        // Sort the references in the toDelete ArrayList in descending order to delete from the end of the page first
        if (!(toDelete.size() <= 1)) {
            sortReferencesDescending(toDelete);
        }

        // Looping over the references in the toDelete to get their corresponding unique pages
        for (Ref ref : toDelete) {
            if (!uniquePages.contains(ref.getPage()))
                uniquePages.add(ref.getPage());
        }

        // Stores the references that will be deleted from the toDelete in case they don't match the rest of the conditions in the hashtable
        ArrayList<Ref> toRemoveFromtoDelete = new ArrayList<>();
        // Checking if there are previously indexed references to check whether they match the rest of the conditions in the hashtable
        if (!uniquePages.isEmpty()) {
            for (String fileName : uniquePages) {
                Page page = Page.deserialize(fileName);
                for (Ref ref : toDelete) {
                    if (ref.getPage().equals(fileName)) {
                        boolean removed = page.checkReference(ref, htblColNameValue);
                        if (!removed) {
                            toRemoveFromtoDelete.add(ref);
                        }
                    }
                }
                page.serialize();
                toDelete.removeAll(toRemoveFromtoDelete);
            }
        }

        // There were no indexed columns in the conditions hashtable; handling all the non-indexed columns
        else {
            if (!htblColNameValue.isEmpty()) {
                for (String fileName : tablePages) {
                    Page page = Page.deserialize(fileName);
                    ArrayList<Ref> references = page.deleteTuples(htblColNameValue);
                    intersection(toDelete, references);
                    page.serialize();
                }
            }
        }

        // Delete the reference that was deleted from the table from the B+ tree itself
        // Retrieveing the table's columns
        List<String[]> tableData = csvConverter.getTableMetadata(this.name);
        if (tableData != null && tableData.size() > 0) {
            // Looping over the references that should be deleted, every iteration should delete that reference from all trees that exist on the tables' columns
            for (Ref ref : toDelete) {
                int refIndex = ref.getIndexInPage();
                String refPage = ref.getPage();
                for (String[] line : tableData) {
                    String indexName = csvConverter.getIndexName(name, line[1]);
                    if (!Objects.equals(indexName, "null")) {
                        BPTree b = BPTree.deserialize(name, line[1]);
                        Page page = Page.deserialize(refPage);
                        Tuple tuple = page.getTuple(refIndex);
                        Hashtable<String, Object> values = tuple.getValues();
                        Object key = values.get(line[1]);
                        b.deletingWithShifting((Comparable) key, ref);
                        b.serialize(name, indexName);
                    }
                }
                // After removing the reference from all trees it should be removed from the page's tuples
                Page page = Page.deserialize(refPage);
                page.tuples.remove(refIndex);
                page.serialize();
                // Delete the page in case all its tuples were deleted
                if (page.tuples.isEmpty()) {
                    File file = new File(this.name + ".class");
                    file.delete();
                }
            }
            // Checking if we only have 1 reference corresponding to the Clustering we perform the same steps as above
            if (CIref != null) {
                int CIrefIndex = CIref.getIndexInPage();
                String CIrefPage = CIref.getPage();
                for (String[] line : tableData) {
                    String indexName = csvConverter.getIndexName(name, line[1]);
                    if (!Objects.equals(indexName, "null")) {
                        BPTree b = BPTree.deserialize(name, line[1]);
                        int refIndex = CIref.getIndexInPage();
                        String refPage = CIref.getPage();
                        Page page = Page.deserialize(refPage);
                        Tuple tuple = page.getTuple(refIndex);
                        Hashtable<String, Object> values = tuple.getValues();
                        Object key = values.get(line[1]);
                        b.deletingWithShifting((Comparable) key, CIref);
                        b.serialize(name, indexName);
                    }
                }
                Page page = Page.deserialize(CIrefPage);
                page.tuples.remove(CIrefIndex);
                page.serialize();
                // Delete the page in case all its tuples were deleted
                if (page.tuples.isEmpty()) {
                    File file = new File(this.name + ".class");
                    file.delete();
                }
            }
        }
    }

    // If toDelete had previous references from previous indices in the conditions an intersection is being done over the toDelete's previous indices' references & the new index's references, or just adding the references to the toDelete if it was empty
    public void sortReferencesDescending(ArrayList<Ref> references) {
        Comparator<Ref> refComparator = new Comparator<Ref>() {
            @Override
            public int compare(Ref ref1, Ref ref2) {
                int pageComparison = ref2.getPage().compareTo(ref1.getPage());
                if (pageComparison == 0) {
                    return Integer.compare(ref2.getIndexInPage(), ref1.getIndexInPage());
                }
                return pageComparison;
            }
        };
        Collections.sort(references, refComparator);
    }

    public static void intersection(ArrayList<Ref> list1, ArrayList<Ref> list2) {
        if (list1.isEmpty()) {
            if(list2 != null && !list2.isEmpty())
                list1.addAll(list2);
        } else {
            HashSet<Ref> set = new HashSet<>(list1);
            set.retainAll(list2);
            list1.clear();
            list1.addAll(set);
        }
    }

    // =======================================================================================================================================
    //  Table updates
    // =======================================================================================================================================

    /**
     * @param clusteringKeyValue
     * @param ColNameType
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void updateTable(Object clusteringKeyValue, Hashtable<String, Object> ColNameType) throws IOException, ClassNotFoundException, DBAppException {

        // Find the page where the row with the clustering key value is located
//        Page page = findPage(clusteringKeyValue);

        // Find the Page
        Page page = this.binarySearch(clusteringKeyValue.toString());

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
            Use the B+Tree to get the set of pages to search in

            Q: how can I search for the tuple using the B+ tree if I dont have the value of the indexedColName?!
            A: I probably cannot do it using the B+ tree, so just do it using binary search.

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
        Page page = this.binarySearch(clusteringKeyValue.toString());


        if (page == null) {
            throw new DBAppException("Page not found :)");
        }

        // initialize old Ref
        Hashtable htblIdxTuple = page.binarySearchPage(clusteringKeyValue.toString(), clusteringDataType);
        if (htblIdxTuple.size() == 0) {
            // entry not found
            System.out.println(clusteringKeyValue + " was not found in " + this.name);
            return;
        }
        int indexInPage = (Integer) htblIdxTuple.keySet().iterator().next();

        // get the old value of the indexed column
        Tuple tupleToUpdate = (Tuple) htblIdxTuple.get(indexInPage);
        Object oldValue = tupleToUpdate.getValues().get(indexedColName);
        Object newValue = htblColNameType.get(indexedColName);

        // Initializes refs
        Ref oldRef = new Ref(page.name, indexInPage);
        Ref newRef = new Ref(page.name, indexInPage);

        // update B+ tree
        tree.update((Comparable) oldValue, (Comparable) newValue, oldRef, newRef);

        // update in place
        boolean isUpdated = page.updateTuple(oldRef, htblColNameType);
        if (!isUpdated) {
            System.out.println("updateIndexedTable failed");
        }

        tree.serialize(this.name, "B+ Tree");
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

    private Page findPageIndexed(ArrayList<Ref> searchSet, Object clusteringKeyValue) throws IOException, ClassNotFoundException {
        Page page = null;
        for (Ref ref : searchSet) {
            String pageName = ref.getPage();
            Object pageMax = this.getPageMax(pageName);
            Object pageMin = this.getPageMin(pageName);
            Object clusteringKeyValueCasted = clusteringKeyValue;

            if (compareValues(clusteringKeyValueCasted, pageMax, pageMin)) {
                page = Page.deserialize(pageName);
                break;
            }

        }
        return page;
    }

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


    // =======================================================================================================================================
    //  Table searching
    // =======================================================================================================================================

    public ArrayList<Tuple> searchTable(String columnName, String operator, Object value) throws DBAppException {
        ArrayList<Tuple> results = new ArrayList<>();
        ArrayList<Tuple> pageResults = new ArrayList<Tuple>();
        boolean clustering=csvConverter.isClusteringKey(this.name,columnName);
        // Linear searching
        if (!clustering||!csvConverter.getIndexName(this.name, columnName).equals("null")) {
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
                try {
                    Page page = this.binarySearch(value.toString());
                    if (page!=null)
                        pageResults = page.binarysearchPage(columnName, value, operator);
                    results.addAll(pageResults);
                } catch(IOException | ClassNotFoundException e){
                    e.printStackTrace();
                }
            }
            if(operator.equals("!=")){
                for (String pagename : tablePages) {
                    try {
                        Page page = Page.deserialize(pagename);
                        pageResults = new ArrayList<Tuple>();
                        for(int i=0;i<page.tuples.size();i++){
                            pageResults.add(page.tuples.get(i));
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    results.addAll(pageResults);
                }
                int left = 0;
                int right = results.size() - 1;
                while (left <= right) {
                    int mid = left + (right - left) / 2;
                    Tuple tuple = results.get(mid);
                    Comparable tupleValue = (Comparable) tuple.getValues().get(columnName);
                    int cmp = tupleValue.compareTo(value);
                    if (cmp == 0) {
                        results.remove(mid);
                    } else if (cmp < 0) {
                        left = mid + 1;
                    } else {
                        right = mid - 1;
                    }
                }
            }
            if (operator.equals(">") || operator.equals(">=")) {
                for (int i = tablePages.size() - 1; i >= 0; i--) {
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i));
                        Object maximum = page.max;
                        if (((Comparable) maximum).compareTo(value) > 0) {
                            pageResults = new ArrayList<Tuple>();
                            pageResults = page.binarysearchPage(columnName, value, operator);
                            page.serialize();
                            results.addAll(pageResults);
                        } else {
                            break;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (operator.equals("<") || operator.equals("<=")) {
                for (int i = 0; i < tablePages.size(); i++) {
                    try {
                        Page page = Page.deserialize(this.tablePages.get(i));
                        Object minimum = page.min;
                        if (((Comparable) minimum).compareTo(value) < 0) {
                            pageResults = new ArrayList<Tuple>();
                            pageResults = page.binarysearchPage(columnName, value, operator);
                            page.serialize();
                            results.addAll(pageResults);
                        } else {
                            break;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return results;
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

            // Compare midClusteringKey with newValue
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


    // =======================================================================================================================================
    //  Helpers
    // =======================================================================================================================================

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




    // =======================================================================================================================================
    //  Getters, Setters and toString
    // =======================================================================================================================================

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

    public int getPageCount() {
        return tablePages.size();
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




    public static void main(String[] args) throws IOException {
        ArrayList<Ref> x = new ArrayList<>();
        x.add(new Ref("a",1));
        x.add(new Ref("a",1));
        x.add(new Ref("b",2));
        x.add(new Ref("a",2));
        x.add(new Ref("b",2));
        x.add(new Ref("b",3));
        x.add(new Ref("c",3));
        //orderByPageAndIndexDescending(x);
        for (int i =0; i< x.size(); i++)
        System.out.println(x.get(i));
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
