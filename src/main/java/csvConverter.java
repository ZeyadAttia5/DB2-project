import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
public class csvConverter {
    public static void convert(Hashtable<String, String> hashtable, String tableName) {
        try (FileWriter writer = new FileWriter("metaData/" + tableName + ".csv")) {
            // Write header to CSV file
            writer.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType\n");

            // Iterate over the hashtable entries
            Enumeration<String> keys = hashtable.keys();
            while (keys.hasMoreElements()) {
                String columnName = keys.nextElement();
                String columnType = hashtable.get(columnName);

                // Handle null values
                if (columnType == null) {
                    columnType = "null";
                }

                // Write hashtable entry to CSV file
                writer.append(tableName).append(",") // Table Name
                        .append(columnName).append(",") // Column Name
                        .append(columnType).append(",") // Column Type
                        .append("null").append(",") // ClusteringKey
                        .append("null").append(",") // IndexName
                        .append("null"); // IndexType
                writer.append("\n");
            }

            System.out.println("CSV file successfully created at: " + "metaData/" + tableName + ".csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//    public static void main(String[] args){
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        convert(htblColNameType,"test");
//    }
}
