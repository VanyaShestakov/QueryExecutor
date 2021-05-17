package QueryExecutor.Record.Exceptions;

public class FieldNameDoesNotExistsException extends RuntimeException {
    public FieldNameDoesNotExistsException() {
        super();
    }

    public FieldNameDoesNotExistsException(String message) {
        super(message);
    }

    public FieldNameDoesNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FieldNameDoesNotExistsException(Throwable cause) {
        super(cause);
    }

    protected FieldNameDoesNotExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
