import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;



public class Main {
    public static void main(String[] args) throws DBAppException {
        //getting input from user
//        Scanner scanner = new Scanner(System.in);
//        System.out.print("Enter a SQL statement: ");
//        String sql = scanner.nextLine();
//        scanner.close();
        // Example SQL statement to parse
        //String sql = "SELECT name FROM Student WHERE age >= 5 AND id > 99 OR id < 67;";
        //String sql = userInput;
        // Create a CharStream from the SQL string
        // Example SQL statements to parse
        String selectSql ="SELECT name FROM Student WHERE age >= 5 AND id > 99 OR id < 67;";
        String createIndexSql = "CREATE INDEX name_index ON Employee (name);";

        // Create a CharStream from the SQL strings
        CharStream selectStream = CharStreams.fromString(selectSql);
        CharStream createIndexStream = CharStreams.fromString(createIndexSql);

        // Create a lexer and parser for each SQL statement
        SQLiteLexer selectLexer = new SQLiteLexer(selectStream);
        SQLiteParser selectParser = new SQLiteParser(new CommonTokenStream(selectLexer));

        SQLiteLexer createIndexLexer = new SQLiteLexer(createIndexStream);
        SQLiteParser createIndexParser = new SQLiteParser(new CommonTokenStream(createIndexLexer));

        // Parse the SQL statements and get the parse trees
        SQLiteParser.Sql_stmtContext selectParseTree = selectParser.sql_stmt();
        SQLiteParser.Sql_stmtContext createIndexParseTree = createIndexParser.sql_stmt();

        // Create a CustomListener instance
        CustomListener listener = new CustomListener();

        // Traverse the parse trees with the custom listener
        ParseTreeWalker walker = new ParseTreeWalker();

        if (selectSql.contains("SELECT") || selectSql.contains("select")) {
            walker.walk(listener, selectParseTree);
            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[listener.Select_operators.size()];
            for(int i = 0 ; i < arrSQLTerms.length ; i++)
                arrSQLTerms[i] = new SQLTerm();
            List<List<String>> whereStatements = listener.Select_parsedConditions;

            for (int i = 0; i < whereStatements.size(); i++) {
                arrSQLTerms[i]._strTableName = listener.tableName;
                arrSQLTerms[i]._strColumnName = whereStatements.get(i).get(0);
                arrSQLTerms[i]._strOperator = whereStatements.get(i).get(1);

                String valueStr = whereStatements.get(i).get(2);
                Object parsedValue;

                try {
                    if (valueStr.matches("-?\\d+")) { // Check if the value is an integer
                        parsedValue = Integer.parseInt(valueStr);
                    } else if (valueStr.matches("-?\\d+(\\.\\d+)?")) { // Check if the value is a double
                        parsedValue = Double.parseDouble(valueStr);
                    } else {
                        throw new NumberFormatException("Value is not a valid integer or double: " + valueStr);
                    }
                } catch (NumberFormatException e) {
                    throw new NumberFormatException("Failed to parse value: " + e.getMessage());
                }

                arrSQLTerms[i]._objValue = parsedValue;
            }
            System.out.println(Arrays.toString(arrSQLTerms));
            String[] stringArray = new String[listener.Select_logical_operators.size()];
            for (int i = 0; i < listener.Select_logical_operators.size(); i++) {
                stringArray[i] = listener.Select_logical_operators.get(i);
            }
        }

//        DBApp app = new DBApp();
//        System.out.println(arrSQLTerms[0]._strOperator);
//        app.selectFromTable(arrSQLTerms, stringArray);

        //walking the create index tree
        if (createIndexSql.contains("create") || createIndexSql.contains("CREATE")) {
            walker.walk(listener, createIndexParseTree);
        }
      //  app.createIndex(listener.tableName, listener.createIndexColumns.get(0), listener.indexName);

    }


}
