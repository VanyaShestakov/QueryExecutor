package QueryExecutor;

import Databases.Exceptions.ConnectionIsClosedException;
import QueryExecutor.Exceptions.IncorrectDBRecordException;
import Tools.Pair;
import Tools.TestDBRecord;
import com.mysql.cj.jdbc.Driver;
import java.sql.*;
import java.util.*;

/**<h1>MySQLQueryExecutor class</h1>
 * <h2>
 * The class simplifies making queries to databases MySQL.
 * For the convenience of using the class, special parameters are introduced (records).
 * </h2>
 * <h3>
 * {@code record param} ({@link Recordable})- Any class whose field names must exactly match the names of the columns in the database table.
 * All fields of this class must be public.
 * The class should not have getters and setters.
 * The class must have a public full (sets values for all fields) constructor.
 * The class must implement interface {@link Recordable}
 * </h3>
 * <h3>
 * {@code record param} ({@link Map}) - Any Map whose keys values must exactly match the names of the columns in the database table.
 * </h3>
 *
 */
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
     * This method give an opportunity to execute SQL function: <h2>INSERT</h2>
     * @param tableName string representation of the table name;
     * @param record a special class corresponding to a specific table.
     *               The record description rules for the table are specified in the class description.
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws IncorrectDBRecordException if object of record does not match the corresponding table.
     * The record description rules for the table are specified in the class description.
     */
    public void insert(String tableName, Recordable record) throws SQLException {
        checkConnection();
        checkRecord(record);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
        Pair<String, String> matches = getMatches(rs.getMetaData(), record);
        statement.execute("INSERT INTO " + tableName + " " + matches.getKey() + " VALUES " + matches.getValue());
        statement.close();
    }

    /**This method give an opportunity to execute SQL function: <h2>INSERT</h2>
     * @param tableName string representation of the table name;
     * @param record a {@link Map} corresponding to a specific table.
     *               The record description rules for the table are specified in the class description.
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws IncorrectDBRecordException if object of record does not match the corresponding table.
     * The record description rules for the table are specified in the class description.
     */
    public void insert(String tableName, Map<String, Object> record) throws SQLException {
        checkConnection();
        checkRecord(record);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
        Pair<String, String> matches = getMatches(rs.getMetaData(), record);
        statement.execute("INSERT INTO " + tableName + " " + matches.getKey() + " VALUES " + matches.getValue());
        statement.close();
    }


    /**
     * This method give an opportunity to execute SQL function: <h2>SELECT *</h2>
     * @param tableName string representation of the table name;
     * @return {@code ResultSet}
     * @throws SQLException
     * @throws ConnectionIsClosedException
     */
    public ResultSet select(String tableName) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        ResultSet returnedSet = statement.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData resultSetMetaData = returnedSet.getMetaData();
        System.out.println(resultSetMetaData.getColumnTypeName(1));
        statement.close();
        return returnedSet;
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>TRUNCATE (TABLE)</h2>
     * @param tableName string representation of the table name;
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method
     *      * throws this Exception
     */
    public void truncate(String tableName) throws SQLException, ConnectionIsClosedException {
        checkConnection();
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

    private Pair<String, String> getMatches(ResultSetMetaData rsmd, Object record) throws SQLException {
        int colCount = rsmd.getColumnCount();
        List<Pair<String, String>> pairList = new ArrayList<>();
        Class<?> recClass = record.getClass();
        StringJoiner fieldNames = new StringJoiner(", ", "(", ")");
        StringJoiner values = new StringJoiner(", ", "(", ")");
        try {
            for (int i = 1; i <= colCount; i++) {
                String columnName = rsmd.getColumnName(i);
                Object value = recClass.getDeclaredField(columnName).get(record);
                fieldNames.add(columnName);
                if (value instanceof Number) {
                    values.add(value.toString());
                }else if (value == null) {
                    values.add("null");
                } else {
                    values.add("'" + value + "'");
                }
            }
        } catch (NoSuchFieldException e) {
            throw new IncorrectDBRecordException("Record fields do not match column names of DB", e.getCause());
        } catch (IllegalAccessException e) {
            throw new IncorrectDBRecordException("Not all fields are specified with an access modifier 'public'", e.getCause());
        }
        return new Pair<>(fieldNames.toString(), values.toString());
    }
    private Pair<String, String> getMatches(ResultSetMetaData rsmd, Map<String, Object> data) throws SQLException {
        int colCount = rsmd.getColumnCount();
        List<Pair<String, String>> pairList = new ArrayList<>();
        StringJoiner colNames = new StringJoiner(", ", "(", ")");
        StringJoiner values = new StringJoiner(", ", "(", ")");
            for (int i = 1; i <= colCount; i++) {
                String columnName = rsmd.getColumnName(i);
                Object value = data.get(columnName);
                colNames.add(columnName);
                if (value instanceof Number) {
                    values.add(value.toString());
                }else if (value == null) {
                    values.add("null");
                } else {
                    values.add("'" + value + "'");
                }
            }
        return new Pair<>(colNames.toString(), values.toString());
    }

    private void checkConnection() throws SQLException {
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
    }

    private void checkRecord(Object record) {
        if (record == null) {
            throw new IncorrectDBRecordException("Object of DB record is null");
        }
    }

}
