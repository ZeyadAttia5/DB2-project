import java.io.*;
import java.io.FileReader;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page
{
    public String name;
    public Vector<Tuple> tuples;
    public int maxSize;
    public int max;
    public int min;
    public String clusteringKey;

    public Page(String tableName, int count, String clusteringKey)
    {
        this.name = tableName + "_" + count;
        this.tuples = new Vector<>();
        this.maxSize = readConfigFile();
        this.clusteringKey = clusteringKey;
    }

    public static Vector<String[]> readCSV(String tableName)
    {
        String csvFilePath = "src/metadata/metadata.csv";
        Vector<String[]> result = new Vector<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(","); // Assuming comma (,) as delimiter
                if(columns[0].equals(tableName))
                    result.add(columns);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void insert(Tuple tuple) throws DBAppException, IOException, ClassNotFoundException {
        String name = (this.name.split("_"))[0];
        Vector<String[]> metadata = readCSV(name);
        String datatype = "";
        for(String[] arr : metadata)
        {
            if(arr[3].equals("True"))
                datatype = arr[2].split(".")[2];
        }

        int low = 0;
        int high = this.tuples.size()-1;
        Object value = tuple.values.get(this.clusteringKey);
        System.out.println(value);
        while(low <= high)
        {
            int mid = low + (high - low)/2;
            Tuple midTuple = this.tuples.get(mid);
            if(datatype.equals("String"))
            {
                String midValue = (String) midTuple.values.get(this.clusteringKey);
                if (midValue.equals(value)) {
                    low = mid; // Value already exists
                    break;
                } else if (midValue.compareTo((String) value) < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            else if (datatype.equals("Double"))
            {
                double midValue = (Double) midTuple.values.get(this.clusteringKey);
                if (midValue == (Double) value) {
                    low = mid; // Value already exists
                    break;
                } else if (midValue < (Double) value) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }

            }
            else
            {
                int midValue = (Integer) midTuple.values.get(this.clusteringKey);
                if (midValue == (Integer) value) {
                    low = mid; // Value already exists
                    break;
                } else if (midValue < (Integer) value) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        }
        System.out.println(low);
        if(low>this.tuples.size()-1)
            this.tuples.add(tuple);

        else if(this.tuples.get(low)!=null)
            this.tuples.add(low,tuple);

        Page currPage = this;
        while(currPage.tuples.size()>maxSize)
        {
            Tuple temp = currPage.tuples.lastElement();
            currPage.tuples.removeLast();
            currPage.serialize();
            String currName = currPage.name;
            int currInt = (int) currName.charAt(currName.length()-1);
            currName = currName.replace((char) currInt,(char) (currInt+1));
            currPage = deserialize(currName);
            currPage.tuples.addFirst(temp);
            currPage.serialize();
        }
    }

    public void delete(int index)
    {
        this.tuples.remove(index);
        if(this.tuples.isEmpty())
        {
            File file = new File(this.name + ".class");
            file.delete();
        }
    }

    public int readConfigFile(){
        Properties properties = new Properties();
        String fileName = "src/resources/DBApp.config";
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
        FileOutputStream fileOut = new FileOutputStream(this.name + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    public static Page deserialize(String fileName) throws IOException, ClassNotFoundException {
        Page page;
        FileInputStream fileIn = new FileInputStream(fileName + ".class");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        page = (Page) in.readObject();
        in.close();
        fileIn.close();
        return page;
    }

    @Override
    public String toString()
    {
        String result = "";
        for(Tuple tuple : this.tuples)
            result += tuple.toString() + "///";
        return result;
    }

    public static void main(String[] args) throws DBAppException {
//        Page page = new Page("Bike",2, "id");
//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 1 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        Tuple t = new Tuple(htblColNameValue);
//
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
}
