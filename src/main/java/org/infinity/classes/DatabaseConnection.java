package org.infinity.classes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConnection {
    private static final String URL = "jdbc:postgresql://localhost:5432/mp_batch_data";
    private static final String USER = "postgres";
    private static final String PASSWORD = "138521";

    public static Connection getConnection() throws Exception{
        return DriverManager.getConnection(URL,USER,PASSWORD);
    }

    static boolean tableExists(Connection connection, String tableName) throws SQLException {
        // Query to check if the table exists
        String query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + tableName + "')";

        try (Statement statement = connection.createStatement()) {
            // Execute the query
            var resultSet = statement.executeQuery(query);
            resultSet.next();

            // Return true if the table exists, false otherwise
            return !resultSet.getBoolean(1);
        }
    }
}
