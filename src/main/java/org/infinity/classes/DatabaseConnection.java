package org.infinity.classes;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnection {
    private static final String URL = "jdbc:postgresql://localhost:5432/your_database_name";
    private static final String USER = "your_username";
    private static final String PASSWORD = "your_password";

    public static Connection getConnection() throws Exception{
        return DriverManager.getConnection(URL,USER,PASSWORD);
    }
}
