import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.io.*;
import java.util.*;

public class Metadata {
    public static void writeToCSV(Hashtable<String, String> hashtable, String tableName, String strClusteringKeyColumn) {
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

                // Determine if the column is the clustering key
                boolean isClusteringKey = columnName.equalsIgnoreCase(strClusteringKeyColumn);

                // Write hashtable entry to CSV file
                writer.append(tableName).append(",") // Table Name
                        .append(columnName).append(",") // Column Name
                        .append(columnType).append(",") // Column Type
                        .append(isClusteringKey ? "True" : "False").append(",") // ClusteringKey
                        .append("null").append(",") // IndexName
                        .append("null"); // IndexType
                writer.append("\n");
            }

            System.out.println("CSV file successfully created at: " + "metaData/" + tableName + ".csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean isClusteringKey(String tableName, String columnName) {
        // Implement your logic to determine if columnName is the clustering key for tableName
        // For simplicity, assuming the column named "ID" is the clustering key for all tables
        return tableName.equalsIgnoreCase(tableName) && columnName.equals(columnName);
    }

    public static String getClusteringKey(String tableName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4 && parts[0].equalsIgnoreCase(tableName) && parts[3].equalsIgnoreCase("True")) {
                    return parts[1]; // Return the clustering key column name
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null; // No clustering key found for the table
    }

    public static List<String[]> getTableMetadata(String tableName) {
        List<String[]> tableMetadata = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 6 && parts[0].equalsIgnoreCase(tableName)) {
                    tableMetadata.add(parts);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableMetadata;
    }
}
