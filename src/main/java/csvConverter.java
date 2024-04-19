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

        for(String key : hashtable.keySet())
        {
            String dataType = hashtable.get(key);
            String[] separatedDataType = dataType.split("\\.");
            separatedDataType[0] = separatedDataType[0].toLowerCase();
            separatedDataType[1] = separatedDataType[1].toLowerCase();
            separatedDataType[2] = Character.toUpperCase(separatedDataType[2].charAt(0)) + separatedDataType[2].substring(1);
            hashtable.put(key,String.join(".",separatedDataType));
        }

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

    //returns the data type of the column
    public static String getColumnType(String tableName, String columnName){

        try (BufferedReader reader1 = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader1.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields[0].equals(tableName) && fields[1].equals(columnName))
                    return fields[2];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    // Adding index to metadata
    public static void addIndexToMetadata(String strTableName, String strColName, String strIndexName) throws DBAppException {

        // Validates table, column and index name
        ValidateTableColumnIndex(strTableName, strColName, strIndexName);

        // Updating metadata
        boolean updated = updateIndexCSV(strTableName, strColName, strIndexName);

        // Handles case where column already has an index, hence cannot create a new one
        if(!updated)
            throw new DBAppException("There already exists an index for this column in this table.");
    }

    private static boolean updateIndexCSV(String strTableName, String strColName, String strIndexName) {
        List<String> lines = new ArrayList<>(); // List to hold modified lines
        boolean updated = false; // Flag to track if metadata was updated
        try (BufferedReader reader = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            // Read each line from the metadata file
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                // Check if the current line matches the specified table name, column name, and has no index
                if (fields[0].equals(strTableName) && fields[1].equals(strColName) && fields[4].equals("null")) {
                    updated = true; // Set the flag to indicate metadata was updated
                    fields[4] = strIndexName; // Update the index name
                    fields[5] = "B+Tree";
                    line = String.join(",", fields); // Reconstruct the line with updated fields
                }
                lines.add(line); // Add the modified line to the list
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Write the modified metadata back to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(METADATA_FILE))) {
            for (String modifiedLine : lines) {
                writer.write(modifiedLine);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updated;
    }

    private static void ValidateTableColumnIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
        HashSet<String> existingIndices = new HashSet<>(); // Names of all indices for table of interest
        HashSet<String> existingColumns = new HashSet<>(); // Names of all columns for table of interest

        // Reading metadata file
        try (BufferedReader reader1 = new BufferedReader(new FileReader(METADATA_FILE))) {
            String line;
            while ((line = reader1.readLine()) != null) {
                String[] fields = line.split(",");
                // Accumulating all index names for table of interest
                if (fields[0].equals(strTableName))
                    existingIndices.add(fields[4]);
                // Accumulating all column names for table of interest
                if(fields[0].equals(strTableName))
                    existingColumns.add(fields[1]);
                // Breaking once we accessed all values for table of interest
                if (!fields[0].equals(strTableName) && !existingIndices.isEmpty())
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Throwing exception if table doesn't exist
        if(existingIndices.isEmpty())
            throw new DBAppException("Table " + strTableName + " does not exist.");

        // Throwing exception if column doesn't exist
        if(!existingColumns.contains(strColName))
            throw new DBAppException("Column " + strColName + " does not exist.");

        // Throwing exception if index name already exists
        if(existingIndices.contains(strIndexName))
            throw new DBAppException("Index " + strIndexName + " already exists in " + strTableName);
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
