package QueryExecutor;
import QueryExecutor.Exceptions.IncorrectRecordException;
import QueryExecutor.Record.Record;
import QueryExecutor.WhereExpression.WhereExpression;
import org.junit.jupiter.api.*;


import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

class MySQLQueryExecutorTest {

    static MySQLQueryExecutor executor;

    @BeforeAll
    static void initExecutor() throws SQLException {
        executor = new MySQLQueryExecutor("test_database", "admin", "admin");
        executor.truncate("junit_table");
    }

    @AfterAll
    static void closeExecutor() throws SQLException {
        executor.close();
    }


    @Test
    @DisplayName("insert test: should insert a correct record into db table")
    void shouldInsertIfRecordIsCorrect() throws SQLException {
        Record record = getCorrectRecord();
        executor.insert("junit_table", record);
        WhereExpression we = new WhereExpression();
        we.addCondition("age=4").and("name='Ivan'");
        List<Record> recs =  executor.select("junit_table", we);
        Assertions.assertEquals(record, recs.get(0));
    }

    @Test
    @DisplayName("insert test: should throws IncorrectRecordException " +
            "if record fields does not match the corresponding table ")
    void shouldThrowExceptionIfRecordIsIncorrect() throws SQLException {
        Record record = getIncorrectRecord();
        Assertions.assertThrows(IncorrectRecordException.class,
                () -> executor.insert("junit_table", record));
    }

    @Test
    void update() {
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