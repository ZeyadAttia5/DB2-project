import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;

import java.util.ArrayList;
import java.util.List;


public class CustomListener extends SQLiteParserBaseListener {
    public String tableName;
    public String columnName;
    public List<String> Select_logical_operators;
    public List<String> Select_operators;
    public List<List<String>> Select_parsedConditions;
    public List<String> createIndexColumns;
    public String indexName;
    public List<String> createTableColnames;
    public List<String> createTableColtypes;


    @Override
    public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
        System.out.println("Entering select_stmt: " + ctx.getText());

        // Extract table name
        tableName = ctx.select_core(0).table_or_subquery(0).table_name().any_name().getText();

        // Extract column name
        columnName = "*"; // Default value for SELECT *
        SQLiteParser.Result_columnContext resultColumn = ctx.select_core(0).result_column(0);
        if (resultColumn != null && resultColumn.expr() != null && resultColumn.expr().column_name() != null) {
            columnName = resultColumn.expr().column_name().any_name().getText();
        }

        // Extract conditions from WHERE clause
        SQLiteParser.ExprContext exprContext = ctx.select_core(0).whereExpr;
        String conditions = extractConditions(exprContext);

        // Print extracted information
        System.out.println("Table Name: " + tableName);
        System.out.println("Column Name: " + columnName);
        System.out.println("Set operators: " + Select_logical_operators);
        System.out.println("operators: " + Select_operators);
        System.out.println("Where statement: " + Select_parsedConditions);

    }

    public String extractConditions(SQLiteParser.ExprContext exprContext) {
        if (exprContext == null) {
            return ""; // No conditions
        }


        List<String> NonlogicalOperators = new ArrayList<>();
        StringBuilder conditions = new StringBuilder();
        List<String> logicalOperators = new ArrayList<>();
        extractConditionsRecursive(exprContext, conditions, logicalOperators);
        NonlogicalOperators = filterNonLogicalOperators(logicalOperators);
        removeLogicalOperators(conditions);
        removeDoubleSpaces(conditions);
        List<String> conditionsList = parseConditionsString(conditions.toString());
        List<List<String>> parsedConditions = parseConditionsList(conditionsList);
        Select_logical_operators = logicalOperators;
        Select_operators = NonlogicalOperators;
        Select_parsedConditions = collapseElements(parsedConditions,3);
        return conditions.toString();
    }

    public void extractConditionsRecursive(SQLiteParser.ExprContext exprContext, StringBuilder conditions, List<String> logicalOperators) {
        if (exprContext == null) {
            return;
        }

        // Check if this is a binary expression (e.g., AND, OR)
        if (exprContext.getChildCount() == 3 && exprContext.getChild(1) != null &&
                exprContext.expr(0) != null && exprContext.expr(1) != null) {
            // Left operand
            extractConditionsRecursive(exprContext.expr(0), conditions, logicalOperators);

            // Operator
            String operator = exprContext.getChild(1).getText();
            conditions.append(operator).append(" ");
            logicalOperators.add(operator);

            // Right operand
            extractConditionsRecursive(exprContext.expr(1), conditions, logicalOperators);
        } else if (exprContext.getChildCount() == 3) {
            // Handle case where one of the children is null
            if (exprContext.expr(0) != null) {
                extractConditionsRecursive(exprContext.expr(0), conditions, logicalOperators);
            }

            if (exprContext.expr(1) != null) {
                extractConditionsRecursive(exprContext.expr(1), conditions, logicalOperators);
            }
        } else if (exprContext.getChildCount() == 1) {
            // Base case: leaf node (column = value)
            String condition = exprContext.getChild(0).getText();
            conditions.append(condition).append(" ");
        }
    }


    public List<String> filterNonLogicalOperators(List<String> logicalOperators) {
        List<String> nonLogicalOperators = new ArrayList<>();
        for (int i = 0; i < logicalOperators.size(); i++) {
            String operator = logicalOperators.get(i);
            if (!operator.equalsIgnoreCase("AND") && !operator.equalsIgnoreCase("OR")) {
                nonLogicalOperators.add(operator);
                logicalOperators.remove(i);
            }
        }
        return nonLogicalOperators;
    }

    public static void removeLogicalOperators(StringBuilder conditionsBuilder) {
        // Replace "AND" and "OR" with an empty string
        replaceSubstring(conditionsBuilder, "AND", "");
        replaceSubstring(conditionsBuilder, "OR", "");
    }

    public static void replaceSubstring(StringBuilder stringBuilder, String target, String replacement) {
        int index = stringBuilder.indexOf(target);
        while (index != -1) {
            stringBuilder.replace(index, index + target.length(), replacement);
            index = stringBuilder.indexOf(target, index + replacement.length());
        }
    }

    public static void removeDoubleSpaces(StringBuilder sb) {
        int index = 0;
        while (index < sb.length() - 1) {
            if (sb.charAt(index) == ' ' && sb.charAt(index + 1) == ' ') {
                sb.deleteCharAt(index + 1);
            } else {
                index++;
            }
        }


    }

    public static List<String> parseConditionsString(String conditionsString) {
        // Split the conditions string by spaces
        String[] conditionsArray = conditionsString.split("\\s+");
        List<String> conditionsList = new ArrayList<>();

        // Remove any empty elements from the array
        for (String condition : conditionsArray) {
            if (!condition.isEmpty()) {
                conditionsList.add(condition);
            }
        }
        return conditionsList;
    }

    public static List<List<String>> parseConditionsList(List<String> conditionsList) {
        List<List<String>> parsedConditions = new ArrayList<>();

        // Iterate over each condition in the list
        for (String condition : conditionsList) {
            // Split the condition into column name, operator, and value
            String[] parts = condition.split("\\s+");
            List<String> parsedCondition = new ArrayList<>();

            // Add column name, operator, and value to the parsed condition list
            for (String part : parts) {
                parsedCondition.add(part);
            }
            parsedConditions.add(parsedCondition);
        }
        return parsedConditions;
    }

    public static List<List<String>> collapseElements(List<List<String>> inputList, int size) {
        List<List<String>> collapsedList = new ArrayList<>();

        for (int i = 0; i < inputList.size(); i += size) {
            List<String> sublist = new ArrayList<>();
            for (int j = 0; j < size; j++) {
                sublist.addAll(inputList.get(i + j));
            }
            collapsedList.add(sublist);
        }

        return collapsedList;
    }


    @Override
    public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        // Handle CREATE INDEX statements
        System.out.println("Entering create_index_stmt: " + ctx.getText());

        // Extract index name and table name
        indexName = ctx.index_name().getText();
        tableName = ctx.table_name().getText();

        // Extract column names
        createIndexColumns = new ArrayList<>();
        List<SQLiteParser.Indexed_columnContext> indexedColumns = ctx.indexed_column();
        for (SQLiteParser.Indexed_columnContext indexedColumn : indexedColumns) {
            String columnName = indexedColumn.column_name().getText();
            createIndexColumns.add(columnName);
        }
        String indexType = "B+ Tree";

        // Print extracted information
        System.out.println("Index Type: " + indexType );
        System.out.println("Index Name: " + indexName);
        System.out.println("Table Name: " + tableName);
        System.out.println("Columns: " + createIndexColumns);
    }

    @Override
    public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        // Handle CREATE TABLE statements
        System.out.println("Entering create_table_stmt: " + ctx.getText());

        // Extract table name
        tableName = ctx.table_name().getText();

        // Extract column definitions
        List<SQLiteParser.Column_defContext> columnDefs = ctx.column_def();
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        for (SQLiteParser.Column_defContext columnDef : columnDefs) {
            String columnName = columnDef.column_name().getText();
            String columnType = columnDef.type_name().getText();
            columnNames.add(columnName);
            columnTypes.add(columnType);
        }

        createTableColnames = columnNames;
        createTableColtypes= columnTypes;

        // Print extracted information
        System.out.println("Table Name: " + tableName);
        System.out.println("Column Names: " + columnNames);
        System.out.println("Column Types: " + columnTypes);
    }





}








