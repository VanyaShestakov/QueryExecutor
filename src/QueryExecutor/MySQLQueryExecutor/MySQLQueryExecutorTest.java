package QueryExecutor.MySQLQueryExecutor;
import QueryExecutor.MySQLQueryExecutor.Exceptions.ConnectionIsClosedException;
import QueryExecutor.MySQLQueryExecutor.Exceptions.IncorrectRecordException;
import QueryExecutor.Record.Record;
import QueryExecutor.WhereExpression.WhereExpression;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.List;

class MySQLQueryExecutorTest {

    static MySQLQueryExecutor executor;
    private final static String DB_NAME = "test_database";
    private final static String USERNAME = "admin";
    private final static String PASSWORD = "admin";
    private final static String TABLE_NAME = "junit_table";

    @BeforeAll
    static void initExecutor() throws SQLException {
        executor = new MySQLQueryExecutor(DB_NAME, USERNAME, PASSWORD);
        executor.openConnection();
    }

    @AfterEach
    void clearTable() throws SQLException {
        executor.truncate(TABLE_NAME);
    }

    @AfterAll
    static void closeExecutor() throws SQLException {
        executor.closeConnection();
    }

    @Test
    @DisplayName("insert test: should insert a correct record into db table")
    void shouldInsertIfRecordIsCorrect() throws SQLException {
        Record record = getCorrectRecord();
        executor.insert(TABLE_NAME, record);
        WhereExpression we = new WhereExpression();
        we.addCondition("age=4").and("name='Ivan'");
        List<Record> recs =  executor.select(TABLE_NAME, we);
        Assertions.assertEquals(record, recs.get(0));
    }

    @Test
    @DisplayName("insert test: should throws IncorrectRecordException " +
            "if record fields does not match the corresponding table ")
    void shouldThrowExceptionIfRecordIsIncorrect() throws SQLException {
        Record record = getIncorrectRecord();
        Assertions.assertThrows(IncorrectRecordException.class,
                () -> executor.insert(TABLE_NAME, record));
    }

    @Test
    @DisplayName("insert test: should throws ConnectionIsClosedException " +
            "if executor is closed")
    void shouldThrowExceptionIfConnectionIsClosed() throws SQLException {
        Record record = getCorrectRecord();
        executor.closeConnection();
        Assertions.assertThrows(ConnectionIsClosedException.class,
                () -> executor.insert(TABLE_NAME, record));
        executor.openConnection();
    }

    @Test
    @DisplayName("update test: should throws IncorrectRecordException if record with changes is empty")
    void shouldThrowExceptionIfRecordIsEmpty() throws SQLException {
        Record emptyRecord = new Record();
        Record record = getCorrectRecord();

        WhereExpression expression = new WhereExpression();
        expression.addCondition("name = 'Ivan'");
        executor.openConnection();
        executor.insert(TABLE_NAME, record);
        Assertions.assertThrows(IncorrectRecordException.class,
                () ->  executor.update(TABLE_NAME, emptyRecord, expression));
    }

    private Record getCorrectRecord() {
        Record record = new Record();
        record.addField("id", 1);
        record.addField("name" , "Ivan");
        record.addField("surname" , "Shestakov");
        record.addField("age" , 4);
        record.addField("date", "2020-11-08");
        return record;
    }

    private Record getIncorrectRecord() {
        Record record = new Record();
        record.addField("id", 1);
        record.addField("name" , "Ivan");
        record.addField("surname" , "Shestakov");
        record.addField("salary" , 4);
        record.addField("date", "2020-11-08");
        return record;
    }


}