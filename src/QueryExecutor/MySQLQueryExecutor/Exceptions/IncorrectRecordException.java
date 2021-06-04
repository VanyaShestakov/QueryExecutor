package QueryExecutor.MySQLQueryExecutor.Exceptions;

public class IncorrectRecordException extends RuntimeException {
    public IncorrectRecordException() {
        super();
    }

    public IncorrectRecordException(String message) {
        super(message);
    }

    public IncorrectRecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public IncorrectRecordException(Throwable cause) {
        super(cause);
    }

    protected IncorrectRecordException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
