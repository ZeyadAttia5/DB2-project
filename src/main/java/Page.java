import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

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
        this.maxSize = readConfigFile();
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
            if(result != null)
            {
                BPTree tree = BPTree.deserialize(tableName,colName);
                tree.insert((Comparable) tuple.values.get(colName), reference);
            }
        }
    }

    public Ref insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {

        String[] arr = this.name.split("_");
        String clust = csvConverter.getClusteringKey(arr[0]);





        if (this.tuples.size() == 0) {
            this.tuples.add(tuple);
            this.max = tuple.values.get(clust);
            this.min = this.max;
            this.serialize();
            Ref result = new Ref(this.name, this.tuples.size() - 1);
            insertHelper(result, tuple, arr[0]);
            return result;
        }

        String name = (this.name.split("_"))[0];
        Vector<String[]> metadata = readCSV(name);
        String datatype = "";
        for (String[] array : metadata) {
            if (array[3].equals("True")) {
                datatype = array[2].split("\\.")[2];
            }
        }

        int low = 0;
        int high = this.tuples.size() - 1;
        Object value = tuple.values.get(this.clusteringKey);
        System.out.println(value);
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
        System.out.println(low);
        if (low > this.tuples.size() - 1) {
            this.tuples.add(tuple);
            if (((Comparable) this.max).compareTo(tuple.values.get(clust)) < 0) {
                this.max = tuple.values.get(clust);
            }
            this.serialize();
            Ref result = new Ref(this.name, this.tuples.size() - 1);
            insertHelper(result, tuple, arr[0]);
            return result;
        } else if (this.tuples.get(low) != null) {
            this.tuples.add(low, tuple);
            if (((Comparable) this.max).compareTo(tuple.values.get(clust)) < 0) {
                this.max = tuple.values.get(clust);
            }
            this.serialize();
            Ref result = new Ref(this.name, low);
            insertHelper(result, tuple, arr[0]);
            return result;
        }

        Page currPage = this;
        while (currPage.tuples.size() > maxSize) {
            Tuple temp = currPage.tuples.lastElement();
            currPage.tuples.remove(temp);
            currPage.serialize();
            String currName = currPage.name;
            int currInt = currName.charAt(currName.length() - 1);
            currName = currName.replace((char) currInt, (char) (currInt + 1));
            currPage = deserialize(currName);
            currPage.tuples.add(0, temp);
            currPage.serialize();
        }
        if (((Comparable) this.max).compareTo(tuple.values.get(clust)) < 0) {
            this.max = tuple.values.get(clust);
        }
        this.serialize();
        return new Ref(this.name, low);
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

    public void delete(int index) {
        this.tuples.remove(index);
        if (this.tuples.isEmpty()) {
            File file = new File(this.name + ".class");
            file.delete();
        }
    }

    public int readConfigFile() {
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
            return maxSize = Integer.parseInt(maxSizeStr);
        } else {

            return maxSize = 200; // Default value
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