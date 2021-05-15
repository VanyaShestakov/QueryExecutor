package QueryExecutor.Exceptions;

public class IncorrectDBRecordException extends RuntimeException{
    public IncorrectDBRecordException() {
        super();
    }

    public IncorrectDBRecordException(String message) {
        super(message);
    }

    public IncorrectDBRecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public IncorrectDBRecordException(Throwable cause) {
        super(cause);
    }

    protected IncorrectDBRecordException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
