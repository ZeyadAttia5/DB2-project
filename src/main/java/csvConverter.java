import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class csvConverter {

    private static final String METADATA_FILE = "src/main/resources/metadata.csv";

    public static void createMetaDataFile(){
        File metadataFile = new File(METADATA_FILE);

        // Check if the file exists, and create it if it doesn't
        if (!metadataFile.exists()) {
            try {
                metadataFile.createNewFile();
                System.out.println("Metadata file created: " + METADATA_FILE);
            } catch (IOException e) {
                System.err.println("Error creating metadata file: " + e.getMessage());
                return; // Exit the method if file creation fails
            }
        }
    }

    public static void convert(Hashtable<String, String> hashtable, String tableName, String strClusteringKeyColumn) {
        try (FileReader fileReader = new FileReader(METADATA_FILE);
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter writer = new FileWriter(METADATA_FILE, true)) {

            // Check if the table name already exists in metadata
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(",");
                String existingTableName = parts[0].trim();
                if (existingTableName.equals(tableName)) {
                    System.out.println("Table name '" + tableName + "' already exists in the CSV file. Cannot convert.");
                    return;
                }
            }

            // Write header to CSV file if it's empty
            if (Files.size(Path.of(METADATA_FILE)) == 0) {
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
                if (columnName.equals(strClusteringKeyColumn)) {
                    isCluster = "True";
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
        } catch (FileNotFoundException e) {
            System.err.println("Metadata file not found: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Error reading/writing metadata file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static String getDataType(String strTableName, String strColName) {

        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields[0].equals(strTableName) && fields[1].equals(strColName)) return fields[2];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }


    public static String getIndexName(String tableName, String colName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields[0].equals(tableName) && fields[1].equals(colName)) return fields[4];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //returns a string with the ClusteringKey Column Name
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

    public static boolean isClusteringKey(String tableName, String clusteringKeyValue) {
        return getClusteringKey(tableName).equalsIgnoreCase(clusteringKeyValue);
    }

    //  returns a List<String[]> of the table's metadata
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

    public static boolean tablePresent(String tableName){

        try (BufferedReader reader1 = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader1.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields[0].equals(tableName))
                    return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    // Adjusting the metadata file
    public static boolean addIndexToCSV(String strTableName, String strColName, String strIndexName) {

        // Getting names of all indices for table of interest
        HashSet<String> existingIndex = new HashSet<>();
        try (BufferedReader reader1 = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader1.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields[0].equals(strTableName)) existingIndex.add(fields[4]);
                if (!fields[0].equals(strTableName) && !existingIndex.isEmpty()) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Updating csv file
        List<String> lines = new ArrayList<>();
        boolean found = false;
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;

            while ((line = reader.readLine()) != null) {
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

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(METADATA_FILE))) {
            for (String modifiedLine : lines) {
                writer.write(modifiedLine);
                writer.newLine();
            }

            System.out.println(found ? "Index added." : "This index name already exists in this table.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return found;
    }

    public static void main(String[] args) {
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        convert(htblColNameType,"test", "id");

        // testing with same table name and same index name
//        addIndexToCSV("Girl","gpa","GpaIndex");// should work
//        addIndexToCSV("Girl","id","IdIndex");// should not work bec same name


    }
}
