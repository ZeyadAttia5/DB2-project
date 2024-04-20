import java.io.IOException;
import java.util.*;


public class DBApp {


    public DBApp() {
    }

    public void init() {
        csvConverter.createMetaDataFile();
    }

    // =======================================================================================================================================
    //  Table Creation:
    //  strTableName -> name of table to be created
    //  strClusteringKeyColumn -> column that is the primary key and the clustering column
    //  htblColNameValue<column name, data type> -> hashtable of column names and their data types
    // =======================================================================================================================================
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
            // Check if the table already exists
            if (csvConverter.tablePresent(strTableName))
                throw new DBAppException("This table is already present.");

            // Traverse column data to check validity of data
            HashSet<String> availableColumns = new HashSet<>();
            for (String key : htblColNameType.keySet()) {
                String dataType = htblColNameType.get(key);
                // Invalid data type for any of the columns
                if (!dataType.equalsIgnoreCase("java.lang.string") && !dataType.equalsIgnoreCase("java.lang.double") && !dataType.equalsIgnoreCase("java.lang.integer"))
                    throw new DBAppException("Invalid data type for " + key + " please enter integer, string or double");
                availableColumns.add(key);
            }

            // Attempting to create a clustering key on a column that doesn't exist
            if (!availableColumns.contains(strClusteringKeyColumn))
                throw new DBAppException("Cannot create a clustering key on a column that doesn't exist. Try creating the table again with valid inputs.");

            // Create the metaData.csv file using the hashtable input and store it in the metaData package
            csvConverter.addTableToMetaData(htblColNameType, strTableName, strClusteringKeyColumn);

            // Initialize a new table object
            Table newTable = new Table(strTableName);
            newTable.serialize();

    }


    // =======================================================================================================================================
    //  B+Tree index Creation:
    //  strTableName -> name of table in which index will be created
    //  strColName -> column in which index will be created
    //  strIndexName -> name of index to be created
    // =======================================================================================================================================

    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

        // Adjusting metadata file with new index
        csvConverter.addIndexToMetadata(strTableName, strColName, strIndexName);

        // Retrieving data type for desired column
        String dataType = csvConverter.getDataType(strTableName, strColName);

        // Initialising b+tree
        BPTree tree = null;
        if (dataType.equalsIgnoreCase("java.lang.Integer")) {
            tree = new BPTree<Integer>(3);
        } else if (dataType.equalsIgnoreCase("java.lang.String")) {
            tree = new BPTree<String>(3);
        } else  {
            tree = new BPTree<Double>(3);
        }

        // Populating tree if table is not empty
        Table currentTable = Table.deserialize(strTableName);
        for (String page : currentTable.tablePages) {
            Page currentPage = Page.deserialize(page);
            for (int i = 0; i < currentPage.getTuples().size(); i++) {
                Tuple currentTuple = currentPage.getTuples().get(i);
                Object value = currentTuple.getValues().get(strColName);
                tree.insert((Comparable) value, new Ref(page, i));
            }
        }

        tree.serialize(strTableName, strIndexName);

    }


    // =======================================================================================================================================
    //  Insertion into table:
    //  inserting one row only
    //  strTableName -> name of table you are inserting to
    //  htblColNameValue<column name, value> -> hashtable of columns and their corresponding values to insert. must include a value for the primary key
    // =======================================================================================================================================
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        // Inserting into a table that doesn't exist
        if (!csvConverter.tablePresent(strTableName))
            throw new DBAppException("Cannot insert into a table that doesn't exist.");

        // Making sure clusteringKey exists in table
        String clusteringKey = csvConverter.getClusteringKey(strTableName);
        if(!htblColNameValue.containsKey(clusteringKey))
            throw new DBAppException("You must insert a value for the primary key: " + clusteringKey + ".");

        Table target = Table.deserialize(strTableName);
        Tuple newTuple = new Tuple(htblColNameValue);
        System.out.println(newTuple);
        target.insert(newTuple);
        System.out.println(Page.deserialize(target.tablePages.get(0)));
        target = Table.deserialize(strTableName);
        for (String pageName : target.tablePages) {
            System.out.println("Page name: " + pageName);
            System.out.println("The page: " + Page.deserialize(pageName));
        }
        System.out.println(target.tablePages);
        System.out.println();

    }


    // =======================================================================================================================================
    //  Updating row in table:
    //  updates one row only, cannot update clustering key
    //  strTableName -> name of table you are updating in
    //  strClusteringKeyValue -> value to look for to find the row to update
    //  htblColNameValue<column name, value> -> hashtable of columns and their corresponding values to update
    // =======================================================================================================================================
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        // Check if the table exists
        if (!csvConverter.tablePresent(strTableName))
            throw new DBAppException("Table " +strTableName+" doesn't exist.");

        // Checking if clustering key data type is correct
        checkClusteringType(strTableName, strClusteringKeyValue);

        // Checking if hashtable input is valid
        validHashTableInput(strTableName, htblColNameValue);

        // Get the table
        Table currTable = Table.deserialize(strTableName);

        try {
            for (String column : htblColNameValue.keySet()) {
                String indexName = csvConverter.getIndexName(strTableName, column);
                Hashtable ht = new Hashtable<String, Object>();
                ht.put(column, htblColNameValue.get(column));

                if (!indexName.equalsIgnoreCase("null")) {
                    currTable.updateIndexedTable(column, strClusteringKeyValue, ht);
                } else {
                    currTable.updateTable(strClusteringKeyValue, ht);
                }
            }

        } catch (IOException e) {
            throw new DBAppException("Table not found\n" + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Table not found\n" + e.getMessage());
        }
        //save changes
        currTable.serialize();
    }



    // =======================================================================================================================================
    //  Deleting from table:
    //  can delete many rows
    //  strTableName -> name of table you are inserting to
    //  htblColNameValue<column name, value> -> hashtable of columns and their corresponding values to delete, define entries anded otgether
    // =======================================================================================================================================
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        // Check if the table exists
        if (!csvConverter.tablePresent(strTableName))
            throw new DBAppException("Table " +strTableName+" doesn't exist.");

        validHashTableInput(strTableName, htblColNameValue);

        Table table = Table.deserialize( strTableName);
        table.delete(htblColNameValue);
        table.serialize();

    }


    // =======================================================================================================================================
    //  Selecting from table:
    // =======================================================================================================================================

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {


        // There are no sql terms
        if (arrSQLTerms.length == 0)
            throw new DBAppException("No SQL terms");

        // Number of operators doesn't match the SQL terms count
        if (strarrOperators != null && strarrOperators.length + 1 != arrSQLTerms.length)
            throw new DBAppException("Number of operators doesn't match the SQL terms count");

        String prev=arrSQLTerms[0]._strTableName;
        for(SQLTerm currentTerm: arrSQLTerms){
            if(!currentTerm._strTableName.equals(prev))
                throw new DBAppException("Engine does not support joins");
        }

        ArrayList<ArrayList<Tuple>> res = new ArrayList<>();
        int j = 0;

        for (SQLTerm sqlTerm : arrSQLTerms) {

            String columnName = sqlTerm._strColumnName;
            Object value = sqlTerm._objValue;
            String tableName = sqlTerm._strTableName;
            String operator = sqlTerm._strOperator;
            String columnType = csvConverter.getDataType(tableName, columnName);

            // Selecting from a table that doesn't exist
            if (!csvConverter.tablePresent(tableName))
                throw new DBAppException("Table "+ tableName+" doesn't exist.");

            // Column doesn't exist
            if (columnType == null) {
                throw new DBAppException("Column " + columnName + " not found");
            }

            // If operator is invalid
            if (!sqlTerm.validOperator()) {
                throw new DBAppException("Invalid operator");
            }

            // If types are not compatible
            if (!compatibleTypes(value, columnType)) {
                throw new DBAppException("Datatype of value doesn't match the column datatype: ");
            }
            try {
                Table tableitself = Table.deserialize(tableName);

                // If column has an index and operator is not "!="
                if (!csvConverter.getIndexName(tableName, columnName).equals("null") && !operator.equals("!=")) {

                    ArrayList<Tuple> helper = new ArrayList<>();
                    BPTree ind = BPTree.deserialize(tableName, columnName);
                    ArrayList<Ref> references = new ArrayList<>();
                    int indexInPage = 0;
                    String pagename = "";
                    Page pagenow;
                    switch (operator) {
                        case "=":
                            references = ind.search((Comparable) value);
                            if(references != null){
                                for (int i = 0; i < references.size(); i++) {
                                    pagename = references.get(0).getPage();
                                    pagenow = Page.deserialize(pagename);
                                    indexInPage = references.get(i).getIndexInPage();
                                    helper.add(pagenow.tuples.get(indexInPage));
                                }
                            }
                            res.add(helper);
                            break;
                        case ">=":
                            references = ind.getRefsGreaterEqual((Comparable) value);
                            if(references != null){
                                for (int k = 0; k < references.size(); k++) {
                                    pagename = references.get(k).getPage();
                                    pagenow = Page.deserialize(pagename);
                                    indexInPage = references.get(k).getIndexInPage();
                                    helper.add(pagenow.tuples.get(indexInPage));
                                }
                            }

                            res.add(helper);
                            break;
                        case ">":
                            references = ind.getRefsGreaterThan((Comparable) value);
                            if(references != null){
                                for (int k = 0; k < references.size(); k++) {
                                    pagename = references.get(k).getPage();
                                    pagenow = Page.deserialize(pagename);
                                    indexInPage = references.get(k).getIndexInPage();
                                    helper.add(pagenow.tuples.get(indexInPage));
                                }
                            }
                            res.add(helper);
                            break;
                        case "<=":
                            references = ind.getRefsLessEqual((Comparable) value);
                            if(references != null){
                                for (int k = 0; k < references.size(); k++) {
                                    pagename = references.get(k).getPage();
                                    pagenow = Page.deserialize(pagename);
                                    indexInPage = references.get(k).getIndexInPage();
                                    helper.add(pagenow.tuples.get(indexInPage));
                                }
                            }
                            res.add(helper);
                            break;
                        case "<":
                            references = ind.getRefsLessThan((Comparable) value);
                            if(references != null){
                                for (int k = 0; k < references.size(); k++) {
                                    pagename = references.get(k).getPage();
                                    pagenow = Page.deserialize(pagename);
                                    indexInPage = references.get(k).getIndexInPage();
                                    helper.add(pagenow.tuples.get(indexInPage));
                                }
                            }
                            res.add(helper);
                            break;
                    }
                } else {
                    res.add(tableitself.searchTable(columnName, operator, value));
                }

            } catch (Exception e) {
                throw new DBAppException("Table " + tableName + " not found.");
            }
            if (res.size() > 1) {
                ArrayList<Tuple> l1 = res.remove(0);
                ArrayList<Tuple> l2 = res.remove(0);
                ArrayList<Tuple> midres = new ArrayList<>();
                switch (strarrOperators[j].toUpperCase()) {
                    case "AND":
                        for (Tuple tuple: l1) {
                            for(Tuple tuple2 :l2){
                                if(tuple2.getValues().equals(tuple.getValues())){
                                    midres.add(tuple);
                                }
                            }
                        }
                        res.add(midres);
                        break;
                    case "OR":
                        for (Tuple tuple: l1) {
                            midres.add(tuple);
                        }
                        int mid=midres.size();
                        for(Tuple t2 :l2) {
                            boolean flag = false;
                            for(int i=0;i<mid;i++){
                                if (midres.get(i).getValues().equals(t2.getValues())){
                                    flag=true;
                                }
                            }
                            if(!flag){
                                midres.add(t2);
                            }
                        }
                        res.add(midres);
                        break;
                    case "XOR":
                        for(Tuple t1:l1){
                            boolean flag=false;
                            for(Tuple t2:l2){
                                if(t1.getValues().equals(t2.getValues())){
                                    flag=true;
                                    break;
                                }
                            }
                            if(!flag){
                                midres.add(t1);
                            }
                        }
                        for(Tuple t1:l2){
                            boolean flag=false;
                            for(Tuple t2:l1){
                                if(t1.getValues().equals(t2.getValues())){
                                    flag=true;
                                    break;
                                }
                            }
                            if(!flag){
                                midres.add(t1);
                            }
                        }
                        res.add(midres);
                        break;
                    default:
                        throw new DBAppException("inavalid operator only and, or , xor are allowed");
                }
                j++;
            }
        }
        return res.remove(0).iterator();
    }

    public static void checkClusteringType(String strTableName, String strClusteringKeyValue) throws DBAppException {
        // Find clustering col name
        String clusteringColName = csvConverter.getClusteringKey(strTableName);
        // Find the clustering data type
        String dataType = csvConverter.getDataType(strTableName, clusteringColName);
        Object newValue = null;
        try{
            if (dataType.equalsIgnoreCase("java.lang.integer")) {
                newValue = Integer.parseInt(strClusteringKeyValue);
            } else if (dataType.equalsIgnoreCase("java.lang.string")) {
                newValue = strClusteringKeyValue;
            } else if (dataType.equalsIgnoreCase("java.lang.double")) {
                newValue = Double.parseDouble(strClusteringKeyValue);
            }
        }
        catch (Exception e){
            throw new DBAppException("Incorrect clustering key data type.");
        }
    }

    public static void validHashTableInput(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        List<String[]> tableMetadata = csvConverter.getTableMetadata(strTableName); // Metadata of the table
        // Get all columns in table
        HashSet<String> availableColumns = new HashSet<>();
        for(String[] column : tableMetadata){
            availableColumns.add(column[1]);
        }

        // Handling invalid input
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            // Trying to update a column that doesn't exist
            if(!availableColumns.contains(key))
                throw new DBAppException("Cannot update attribute " + key + " because it doesn't exist.");
            // Trying to update a column with an invalid data type
            if( !compatibleTypes(value, csvConverter.getDataType(strTableName, key)))
                throw new DBAppException("Invalid value: " + value +". Check data type.");
        }
    }

    public static boolean compatibleTypes(Object value, String columnType) {
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

}
