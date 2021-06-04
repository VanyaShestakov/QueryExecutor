package QueryExecutor.MySQLQueryExecutor;

import QueryExecutor.MySQLQueryExecutor.Exceptions.ConnectionIsClosedException;
import QueryExecutor.MySQLQueryExecutor.Exceptions.IncorrectRecordException;
import QueryExecutor.Record.Record;
import QueryExecutor.WhereExpression.WhereExpression;
import Tools.Pair;
import com.mysql.cj.jdbc.Driver;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**<h1>MySQLQueryExecutor class</h1>
 * <h2>
 * The class simplifies making queries to databases MySQL.
 * For the convenience of using the class, special parameters are introduced (records).
 * </h2>
 * <h3>
 * {@code record param} ({@link Record})- a special class that contains fields and their values.
 * For successful operations when working with tables, the record fields must completely match the names of the columns
 * in the database table, otherwise will be thrown {@link IncorrectRecordException}. The Record class is based on the {@link Map}
 * </h3>
 *
 */
public class MySQLQueryExecutor {
    private static final String URL = "jdbc:mysql://localhost:3306/";
    private Connection connection;
    private final String dbName;
    private final String user;
    private final String password;


    /**
     * Constructor registers driver and get connection from {@code DriverManager}
     * @param dbName name of database without full URL to local server
     * @param user username of db user
     * @param password password of db user
     * @throws SQLException
     */
    public MySQLQueryExecutor(String dbName, String user, String password) throws SQLException {
        java.sql.Driver driver = new Driver();
        DriverManager.registerDriver(driver);
        this.dbName = dbName;
        this.user = user;
        this.password = password;
    }

    /**This method give an opportunity to execute SQL function: <h2>INSERT</h2>
     * @param tableName string representation of the table name;
     * @param record a {@link Record} corresponding to a specific table.
     * The record description rules for the table are specified in the class description.
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws IncorrectRecordException if record fields does not match the corresponding table.
     * The record description rules for the table are specified in the class description.
     */
    public void insert(String tableName, Record record) throws SQLException {
        checkConnection();
        checkRecord(record);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
        Pair<String, String> matches = getMatches(rs.getMetaData(), record);
        statement.execute("INSERT INTO " + tableName + " " + matches.getKey() + " VALUES " + matches.getValue());
        statement.close();
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>UPDATE &lt table name &gt SET &lt  col_name &gt = &lt  value &gt ... WHERE &lt condition &gt</h2>
     * @param tableName string representation of the table name;
     * @param record a {@link Record} corresponding to a specific table.
     * The record description rules for the table are specified in the class description.
     * @param whereExpression condition of WHERE SQL keyword ({@link WhereExpression})
     * @throws SQLSyntaxErrorException if param {@code tableName} or {@code whereExpression} does not match data from database
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws IncorrectRecordException if record fields does not match the corresponding table.
     * The record description rules for the table are specified in the class description.
     */
    public void update(String tableName, Record record, WhereExpression whereExpression) throws SQLException {
        checkConnection();
        checkRecord(record);
        Statement statement = connection.createStatement();
        statement.executeUpdate("UPDATE " + tableName + " SET " + buildSetExpression(record) + " WHERE " + whereExpression);
        statement.close();
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>SELECT *</h2>
     * @param tableName string representation of the table name;
     * @return {@link List} of {@link Record}
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws SQLSyntaxErrorException if param {@code tableName} does not match table name from database
     */
    public List<Record> select(String tableName) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
        List<Record> records = getRecordsFromResSet(rs, rs.getMetaData());
        statement.close();
        return records;
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>SELECT * WHERE &lt condition &gt</h2>
     * @param tableName string representation of the table name
     * @param expression condition of WHERE SQL keyword ({@link WhereExpression})
     * @return
     * @throws SQLSyntaxErrorException if param {@code tableName} or {@code expression} does not match data from database
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     */
    public List<Record> select(String tableName, WhereExpression expression) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName + " WHERE " + expression);
        List<Record> records = getRecordsFromResSet(rs, rs.getMetaData());
        statement.close();
        return records;
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>SELECT &lt col_name1 &gt, &lt col_name2 &gt, …</h2>
     * @param tableName string representation of the table name;
     * @param fields list of selected fields names from database table
     * @return {@link List} of {@link Record}
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     */
    public List<Record> select(String tableName, List<String> fields) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        StringJoiner sj = new StringJoiner(",");
        for (String fieldName: fields){
            sj.add(fieldName);
        }
        ResultSet rs = statement.executeQuery("SELECT " + sj + " FROM " + tableName);
        List<Record> records = getRecordsFromResSet(rs, rs.getMetaData());
        statement.close();
        return records;
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>SELECT &lt col_name1 &gt, &lt col_name2 &gt, … WHERE &lt condition &gt</h2>
     * @param tableName string representation of the table name
     * @param fields list of selected fields names from database table
     * @param expression condition of WHERE SQL keyword ({@link WhereExpression})
     * @return {@link List} of {@link Record}
     * @throws SQLSyntaxErrorException if param {@code tableName} or {@code expression} does not match data from database
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     */
    public List<Record> select(String tableName, List<String> fields, WhereExpression expression) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        StringJoiner sj = new StringJoiner(",");
        for (String fieldName: fields){
            sj.add(fieldName);
        }
        ResultSet rs = statement.executeQuery("SELECT " + sj + " FROM " + tableName + " WHERE " + expression);
        List<Record> records = getRecordsFromResSet(rs, rs.getMetaData());
        statement.close();
        return records;
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>DELETE FROM &lt table name &gt </h2>
     * @param tableName string representation of the table name
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws SQLSyntaxErrorException if param {@code tableName} does not match table name from database
     */
    public void delete(String tableName) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        statement.execute("DELETE FROM " + tableName);
        statement.close();
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>DELETE FROM &lt table name &gt WHERE &lt condition &gt </h2>
     * @param tableName string representation of the table name
     * @param expression condition of WHERE SQL keyword ({@link WhereExpression})
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method throws this Exception
     * @throws SQLSyntaxErrorException if param {@code tableName} or {@code expression} does not match data from database
     */
    public void delete(String tableName, WhereExpression expression) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        statement.execute("DELETE FROM " + tableName + " WHERE " + expression);
        statement.close();
    }

    /**
     * This method give an opportunity to execute SQL function: <h2>TRUNCATE (TABLE)</h2>
     * @param tableName string representation of the table name;
     * @throws SQLException
     * @throws ConnectionIsClosedException if connection with database is closed method
     * throws this Exception
     * @throws SQLSyntaxErrorException if param {@code tableName} does not match table name from database
     */
    public void truncate(String tableName) throws SQLException, ConnectionIsClosedException {
        checkConnection();
        Statement statement = connection.createStatement();
        statement.execute("TRUNCATE TABLE " + tableName);
        statement.close();
    }

    /**
     * Method closes connection with database
     * <h3>After working with database connection must be closed!</h4>
     * @throws SQLException
     */
    public void closeConnection() throws SQLException {
        connection.close();
    }

    /**
     * Method opens connection with database
     * <h3>Before working with database connection must be opened!</h4>
     * @throws SQLException
     */
    public void openConnection() throws SQLException {
        connection = DriverManager.getConnection(URL + dbName, user, password);
    }

    /**
     * @return {@code Boolean} Information about connection: if it is closed - true
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    private String buildSetExpression(Record record) {
        Set<String> keys = record.getKeySet();
        StringBuilder sb = new StringBuilder();
        StringJoiner sj = new StringJoiner(", ");
        for (String key: keys) {
            Object value = record.getValue(key);
            String strValue = value instanceof Number ? value.toString() : (value == null ? "null" : "'" + value + "'");
            sj.add(key + "=" + strValue);
        }
        return sj.toString();
    }

    private Pair<String, String> getMatches(ResultSetMetaData rsmd, Record data) throws SQLException {
        int colCount = rsmd.getColumnCount();
        List<Pair<String, String>> pairList = new ArrayList<>();
        StringJoiner colNames = new StringJoiner(", ", "(", ")");
        StringJoiner values = new StringJoiner(", ", "(", ")");
            for (int i = 1; i <= colCount; i++) {
                String columnName = rsmd.getColumnName(i);
                Object value;
                if (data.contains(columnName)) {
                    value = data.getValue(columnName);
                } else {
                    throw new IncorrectRecordException("Field '" + columnName + "' not found in record");
                }
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

    private List<Record> getRecordsFromResSet(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
        int colCount = rsmd.getColumnCount();
        List<Record> returnedList = new ArrayList<>();
        while (rs.next()) {
            Record temp = new Record();
            for (int i = 1; i <= colCount; i++) {
                temp.addField(rsmd.getColumnName(i), rs.getObject(rsmd.getColumnName(i)));
            }
            returnedList.add(temp);
        }
        return returnedList;
    }

    private void checkConnection() throws SQLException {
        if (connection.isClosed()) {
            throw new ConnectionIsClosedException("Connection with database is closed");
        }
    }

    private void checkRecord(Record record) {
        if (record.isEmpty()) {
            throw new IncorrectRecordException("Record has no fields");
        }
    }

    public static void main(String[] args) throws SQLException {
        MySQLQueryExecutor executor = new MySQLQueryExecutor("test_database", "admin", "admin");
        Record record = new Record();
        record.addField("id", 1);
        record.addField("name" , "Sosiska");
        record.addField("date", "2020-11-08");
        record.addField("temperature", 34.4);
        record.insert("test_database", "admin", "admin", "test_table");
    }

}
