import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Page implements Serializable {
    public String name;
    public Vector<Tuple> tuples;
    public int maxSize;
    public Object max;
    public Object min;
    public String clusteringKey;

    public Page(String tableName, int count, String clusteringKey) {
        this.name = tableName + "_" + count;
        this.tuples = new Vector<>();
        this.maxSize = Page.readConfigFile();
        this.clusteringKey = clusteringKey;
    }

    public static Vector<String[]> readCSV(String tableName) {
        String csvFilePath = "src/metadata.csv";
        Vector<String[]> result = new Vector<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(","); // Assuming comma (,) as delimiter
                if (columns[0].equals(tableName))
                    result.add(columns);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Page deserialize(String fileName) throws IOException, ClassNotFoundException {
        Page page;
        String[] arr = fileName.split("_");
        FileInputStream fileIn = new FileInputStream("src/main/resources/tables/" + arr[0] + "/" + fileName + ".class");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        page = (Page) in.readObject();
        in.close();
        fileIn.close();
        return page;
    }

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
//        Page page = new Page("Student",2, "id");
//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 2 ));
//        htblColNameValue.put("name", new String("Yara Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.90 ) );
//        Tuple t = new Tuple(htblColNameValue);
//        page.insert(t);
//        System.out.println(page);
//        page.serialize();
//        System.out.println(deserialize("Student_2"));

//        Hashtable h = new Hashtable( );
//        h.put("id", new Integer( 3 ));
//        h.put("name", new String("Ahmed Noor" ) );
//        h.put("gpa", new Double( 0.95 ) );
//        Tuple t2 = new Tuple(h);
//
//        Hashtable h2 = new Hashtable( );
//        h2.put("id", new Integer( 2 ));
//        h2.put("name", new String("Ahmed Noor" ) );
//        h2.put("gpa", new Double( 0.95 ) );
//        Tuple t3 = new Tuple(h2);
//
//        page.insert(t);
//        page.insert(t2);
//        System.out.println(page);
//
//        page.insert(t3);
//        System.out.println(page);
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public Tuple getTuple(int indexInPage) {
        return this.getTuples().get(indexInPage);
    }

    private void insertHelper(Ref reference, Tuple tuple, String tableName)
    {
        for (String colName : tuple.values.keySet()) {
            String result = csvConverter.getIndexName(tableName,colName);
            if(!result.equals("null"))
            {
                BPTree tree = BPTree.deserialize(tableName,colName);
                tree.insert((Comparable) tuple.values.get(colName), reference);
            }
        }
    }

    public static File[] sortFiles(File[] files) {
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                int num1 = extractNumber(file1.getName());
                int num2 = extractNumber(file2.getName());
                return Integer.compare(num1, num2);
            }

            private int extractNumber(String fileName) {
                // Regex pattern to match underscore followed by digits
                Pattern pattern = Pattern.compile("_(\\d+)\\.class");
                Matcher matcher = pattern.matcher(fileName);
                if (matcher.find()) {
                    return Integer.parseInt(matcher.group(1));
                }
                return -1; // Return -1 if no number found
            }
        });
        return files;
    }

    public Ref insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {

        String[] arr = this.name.split("_");
        Table currTable = Table.deserialize(arr[0]);
        String clust = csvConverter.getClusteringKey(arr[0]);
        Ref result = null;

        if (this.tuples.size() == 0) {
            this.tuples.add(tuple);
            this.max = tuple.values.get(clust);
            this.min = this.max;
            this.serialize();
            currTable.tablePages.add(this.name);
            currTable.pageInfo.put(this.name, new Object[] {this.max, this.min, this.tuples.size()});
            currTable.serialize();
            result = new Ref(this.name, this.tuples.size() - 1);
            insertHelper(result, tuple, arr[0]);
            return result;
        }


        int low = 0;
        int high = this.tuples.size() - 1;
        Object value = tuple.values.get(this.clusteringKey);
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = this.tuples.get(mid);

            Object midValue = midTuple.values.get(this.clusteringKey);
            if (((Comparable) midValue).compareTo(value) == 0) {
                low = mid; // Value already exists
                System.out.println("Can not insert duplicate tuple");
                return null;
            } else if (((Comparable) midValue).compareTo(value) < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        if (low > this.tuples.size() - 1 && ((Comparable) this.tuples.lastElement().values.get(clusteringKey)).compareTo(tuple.values.get(clusteringKey))!=0) {
            this.tuples.add(tuple);
            this.serialize();
            result = new Ref(this.name, this.tuples.size() - 1);
            insertHelper(result, tuple, arr[0]);
        } else if (this.tuples.get(low) != null) {
            this.tuples.add(low, tuple);
            this.serialize();
            result = new Ref(this.name, low);
            insertHelper(result, tuple, arr[0]);
        }
        this.max = this.tuples.lastElement().values.get(clusteringKey);

        if(this.tuples.size()>maxSize) {

            Page currPage = null;
            File pageFolder = new File("src/main/resources/tables/" + arr[0]);
            File[] files = pageFolder.listFiles();
            files = Page.sortFiles(files);
            for (int i = 0; i < files.length; i++) {
                if ((this.name + ".class").equals(files[i].getName())) {
                    files = Arrays.copyOfRange(files, i+1, files.length);
                    break;
                }
            }

            Object[] temp;
            Tuple extra = this.tuples.lastElement();
            this.tuples.remove(extra);
            this.max = (this.tuples.get(this.maxSize-1)).values.get(clusteringKey);
            this.min = (this.tuples.get(0)).values.get(clusteringKey);


            for(File file : files)
            {
                if(extra != null)
                {
                    String fileName = file.getName();
                    currPage = Page.deserialize(fileName.substring(0, fileName.length()-6));
                    currPage.tuples.add(0, extra);
                    currPage.min = currPage.tuples.get(0).values.get(clusteringKey);
                    currPage.max = currPage.tuples.get(currPage.maxSize-1).values.get(clusteringKey);
                    currTable.pageInfo.put(currPage.name, new Object[] {currPage.max, currPage.min, currPage.tuples.size()});
                    currPage.serialize();
                    if(currPage.tuples.size()>maxSize)
                    {
                        extra = currPage.tuples.lastElement();
                        currPage.tuples.remove(extra);
                        currPage.serialize();
                    }
                    else
                        extra = null;
                }
            }

            if(extra!=null)
            {
                Page newPage = new Page(currTable.name, currTable.tablePages.size(), this.clusteringKey);
                newPage.insert(extra);
                newPage.serialize();
                currTable.pageInfo.put(newPage.name, new Object[]{newPage.max, newPage.min, newPage.tuples.size()});
                currTable.tablePages.add(newPage.name);
            }
        }
        currTable.pageInfo.put(this.name, new Object[] {this.max, this.min, this.tuples.size()});
        this.serialize();
        currTable.serialize();
        return result;
    }

    public int binarySearchPage(String clusteringKeyValue, String dataType) throws DBAppException {

        Object newValue = null;
        if (dataType.equalsIgnoreCase("java.lang.integer")) {
            newValue = Integer.parseInt(clusteringKeyValue);
        } else if (dataType.equalsIgnoreCase("java.lang.string")) {
            newValue = (String) clusteringKeyValue;
        } else if (dataType.equalsIgnoreCase("java.lang.double")) {
            newValue = Double.parseDouble(clusteringKeyValue);
        }

        // Check if tuples is null or empty
        if (tuples == null || tuples.isEmpty()) {
            throw new DBAppException("Page tuples are not initialized or empty.");
        }

        // Initialize variables for binary search
        int low = 0;
        int high = tuples.size() - 1;

        // Binary search loop
        while (low <= high) {
            int mid = (low + high) / 2;
            Tuple midTuple = tuples.get(mid);

            // Compare clustering key value in midTuple with clusteringKeyValue
            Object midClusteringKeyValue = midTuple.getValues().get(clusteringKey);
            Comparable<Object> comparableClusteringKeyValue = (Comparable<Object>) newValue;
            int compareResult = comparableClusteringKeyValue.compareTo(midClusteringKeyValue);

            if (compareResult == 0) {
                // Found exact match, return mid
                return mid;
            } else if (compareResult < 0) {
                // If midTuple's clustering key is greater than clusteringKeyValue, search left half
                high = mid - 1;
            } else {
                // If midTuple's clustering key is less than clusteringKeyValue, search right half
                low = mid + 1;
            }
        }

        // If not found, return -1
        return -1;
    }

    public void deleteTuples(Hashtable<String, Object> htblColNameValue) {
        //check if there's a clustering key in the conditions
        if (!htblColNameValue.containsKey(clusteringKey)) {
            //if it's not; we loop through the tuples & for each we check if all the conditions are met
            for (int i = tuples.size() - 1; i >= 0; i--) {
                Tuple tuple = tuples.get(i);
                if (tupleMatchesConditions(tuple, htblColNameValue)) {
                    tuples.remove(i);
                }
            }
        } else{
            //if there's a clustering key we perform binary search to find the tuples then see if they match
            Object clusteringKeyValue = htblColNameValue.get(clusteringKey);
            int index = binarySearch(clusteringKeyValue);
            if (index != -1) {
                //if we found the tuple corresponding to the clustering key, we check the rest of the conditions
                Tuple matchingTuple = tuples.get(index);
                htblColNameValue.remove(clusteringKey);
                if (tupleMatchesConditions(matchingTuple, htblColNameValue)) {
                    tuples.remove(index);
                }
            }
        }
        if (tuples.isEmpty()) {
            File file = new File(this.name + ".class");
            file.delete();
        }
    }

    public boolean deleteIndexedTuples(Ref ref, Hashtable<String, Object> conditions) {
        //we take a tuple reference to check whether it matches the rest of the conditions before deleting the tuple
        int refIndex = ref.getIndexInPage();
        Tuple tuple = tuples.get(refIndex);
        boolean result = false;
        //check if there's a clustering key in the remaining conditions
        if (!conditions.containsKey(clusteringKey)) {
            //if there isn't, we check if the tuple matches the other conditions to delete it
            if (tupleMatchesConditions(tuple, conditions)) {
                tuples.remove(tuple);
                result = true;
            }
        }
        else {
            //if there's a clustering key we perform binary search to find the tuple with the specified clustering key value
            Object clusteringKeyValue = conditions.get(clusteringKey);
            int clusteringIndex = binarySearch(clusteringKeyValue);
            if (clusteringIndex == refIndex) {
                //if we found that the tuple corresponding to the clustering key value is the same as the tuple we are checking we check that they match the rest of the conditions if they exist minus the clustering key column
                conditions.remove(clusteringKey);
                if (!conditions.isEmpty()){
                    if (tupleMatchesConditions(tuple, conditions)) {
                        tuples.remove(tuple);
                        result = true;
                    }
                } else{
                    tuples.remove(tuple);
                }
            }
            if (tuples.isEmpty()) {
                File file = new File(this.name + ".class");
                file.delete();
            }
        }
        return result;
    }

    private boolean tupleMatchesConditions(Tuple tuple, Hashtable<String, Object> conditions) {
        for (String column : conditions.keySet()) {
            Object expectedValue = conditions.get(column);
            Object actualValue = tuple.values.get(column);
            if (!expectedValue.equals(actualValue)) {
                return false;
            }
        }
        return true;
    }

    private int binarySearch(Object clusteringKeyValue) {
        int low = 0;
        int high = tuples.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Tuple midTuple = tuples.get(mid);
            Comparable midValue = (Comparable) midTuple.values.get(clusteringKey);

            int cmp = midValue.compareTo(clusteringKeyValue);
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1;
    }

    /**
     * Updates a tuple in place
     * @param refToOldTuple
     * @param htblColNameType
     * @return true if update was successful, false otherwise
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public boolean updateTuple(Ref refToOldTuple, Hashtable<String, Object> htblColNameType) throws IOException, ClassNotFoundException {
        Page oldPage = Page.deserialize(refToOldTuple.getPage());
        Tuple oldTuple = oldPage.getTuple(refToOldTuple.getIndexInPage());
        boolean isSuccess = false;
        for (String col : htblColNameType.keySet()) {
            if ((oldTuple.getValues().containsKey(col))) {
                //
                oldTuple.getValues().put(col, htblColNameType.get(col));
                isSuccess = true;
            }
            else {
                // do nothing :)
            }
        }
        if (isSuccess == false){
            System.out.println("Couldn't update");
        }
        return isSuccess;
    }

    public static int readConfigFile() {
        Properties properties = new Properties();
        String fileName = "src/main/resources/DBApp.config";
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
            return Integer.parseInt(maxSizeStr);
        } else {

            return 200; // Default value
        }
    }

    public void serialize() throws IOException {
        String[] arr = this.name.split("_");
        FileOutputStream fileOut = new FileOutputStream("src/main/resources/tables/" + arr[0] + "/" + this.name + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    @Override
    public String toString() {
        String result = "";
        for (Tuple tuple : this.tuples)
            result += tuple.toString() + "///";
        return result;
    }
}