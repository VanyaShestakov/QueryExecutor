package QueryExecutor;

import Databases.Exceptions.ConnectionIsClosedException;
import Tools.Pair;
import com.mysql.cj.jdbc.Driver;

import java.sql.*;
import java.util.*;

public class MySQLQueryExecutor {
    private static final String URL = "jdbc:mysql://localhost:3306/";
    private final Connection connection;

    /**
     * Constructor registers driver and get connection from {@code DriverManager}
     * @param dbName name of database without full URL to local server
     * @param user
     * @param password
     * @throws SQLException
     */
    public MySQLQueryExecutor(String dbName, String user, String password) throws SQLException {
        java.sql.Driver driver = new Driver();
        DriverManager.registerDriver(driver);
        this.connection = DriverManager.getConnection(URL + dbName, user, password);
    }

    /**
     * This method give an opportunity to execute SQL function: INSERT
     *
     * @param tableName string representation of the table name;
     * @param data a list of field names and their corresponding inserted values ({@code List<Pair<String, ?>>});
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method
     * throws this Exception
     */
    public void insert(String tableName, List<Pair<String, ?>> data) throws SQLException, ConnectionIsClosedException {
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
        Statement statement = connection.createStatement();
        StringJoiner fieldNames = new StringJoiner(", ", "(", ")");
        StringJoiner values = new StringJoiner(", ", "(", ")");
        for (Pair<String, ?> pair : data) {
            fieldNames.add(pair.getKey());
            if (pair.getValue() instanceof Number) {
                values.add((pair.getValue()).toString());
            } else {
                values.add("'" + pair.getValue() + "'");
            }
        }
        statement.execute("INSERT INTO " + tableName + " " + fieldNames + " VALUES " + values);
        statement.close();
    }

    /**
     * This method give an opportunity to execute SQL function: SELECT *
     * @param tableName string representation of the table name;
     * @return {@code ResultSet}
     * @throws SQLException
     * @throws ConnectionIsClosedException
     */
    public ResultSet select(String tableName) throws SQLException, ConnectionIsClosedException {
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
        Statement statement = connection.createStatement();
        ResultSet returnedSet = statement.executeQuery("SELECT * FROM " + tableName);
        statement.close();
        return returnedSet;
    }


    public ResultSet select(String tableName, List<Pair<String, ?>> whereList) throws SQLException, ConnectionIsClosedException{
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
        Statement statement = connection.createStatement();
        StringJoiner sj = new StringJoiner("," , "(" , ")");
        String el;
        for (Pair<String, ?> pair: whereList) {
            el = pair.getValue() instanceof Number ? pair.getValue().toString() : "'" + pair.getValue() +"'";
            sj.add(pair.getKey() + "=" + el);
        }
       // System.out.println("SELECT * FROM " + tableName + );
        ResultSet returnedSet = statement.executeQuery("SELECT * FROM " + tableName);
        return returnedSet;
    }

    /**
     * This method give an opportunity to execute SQL function: TRUNCATE (TABLE)
     * @param tableName string representation of the table name;
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method
     *      * throws this Exception
     */
    public void truncate(String tableName) throws SQLException, ConnectionIsClosedException {
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
        Statement statement = connection.createStatement();
        statement.execute("TRUNCATE TABLE " + tableName);
        statement.close();
    }

    /**
     * Method closes connection with database
     * @throws SQLException
     */
    public void close() throws SQLException {
        connection.close();
    }

    /**
     * @return {@code Boolean} Information about connection: if it is closed - true
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }
}
