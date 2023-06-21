package org.wwi21seb.vs.group5.communication;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/travelbroker_hotel";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "password";

    private DatabaseConnection() {
    }

    public static Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        return conn;
    }

}
