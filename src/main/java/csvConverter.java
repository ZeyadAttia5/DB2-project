import java.io.*;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class csvConverter {

    public static void convert(Hashtable<String, String> hashtable, String tableName, String strClusteringKeyColumn) {
        try (FileWriter writer = new FileWriter("metadata.csv", true)) {
            // Write header to CSV file
            if  (Files.size(Path.of("metadata.csv")) == 0) {
                writer.write("Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType\n");
            }

            // Iterate over the hashtable entries
            Enumeration<String> keys = hashtable.keys();
            while (keys.hasMoreElements()) {
                String columnName = keys.nextElement();
                String columnType = hashtable.get(columnName);

                // Handle null values
                if (columnType == null) {
                    columnType = "null";
                }

                // Setting cluster key
                String isCluster = "False";
                if(columnName.equals(strClusteringKeyColumn)){
                    isCluster= "True";
                }


                // Write hashtable entry to CSV file
                writer.append(tableName).append(",") // Table Name
                        .append(columnName).append(",") // Column Name
                        .append(columnType).append(",") // Column Type
                        .append(isCluster).append(",") // ClusteringKey
                        .append("null").append(",") // IndexName
                        .append("null"); // IndexType
                writer.append("\n");
            }

            System.out.println("Table metadata successfully added.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    // Adjusting the metadata file
    public static boolean adjustIndexCSV(String strTableName, String strColName, String strIndexName) {

        // Getting names of all indices for table of interest
        HashSet<String> existingIndex = new HashSet<>();
        try (BufferedReader reader1 = new BufferedReader(new FileReader("metadata.csv"))) {
            String line;
            while ((line = reader1.readLine()) != null ) {
                String[] fields = line.split(",");
                if (fields[0].equals(strTableName) )
                    existingIndex.add(fields[4]);
                if(!fields[0].equals(strTableName) && !existingIndex.isEmpty())
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Updating csv file
        List<String> lines = new ArrayList<>();
        boolean found = false;
        try (BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"))) {
            String line;

            while ((line = reader.readLine()) != null ) {
                String[] fields = line.split(",");
                if (fields[0].equals(strTableName) && fields[1].equals(strColName) && !existingIndex.contains(strIndexName)) {
                    found = true;
                    fields[4] = strIndexName;
                    fields[5] = "B+Tree";
                    line = String.join(",", fields);
                }

                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("metadata.csv"))) {
            for (String modifiedLine : lines) {
                writer.write(modifiedLine);
                writer.newLine();
            }

            System.out.println(found ? "Index added." : "Index cannot be added.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return found;
    }

    public static void main(String[] args){
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        convert(htblColNameType,"test", "id");

        // testing with same table name and same index name
        adjustIndexCSV("Girl","gpa","GpaIndex");// should work
        adjustIndexCSV("Girl","id","IdIndex");// should not work bec same name


    }
}
