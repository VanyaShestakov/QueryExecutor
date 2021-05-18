package QueryExecutor.Exceptions;

public class ConnectionIsClosedException extends RuntimeException {
    public ConnectionIsClosedException() {
        super();
    }

    public ConnectionIsClosedException(String message) {
        super(message);
    }

    public ConnectionIsClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionIsClosedException(Throwable cause) {
        super(cause);
    }

    protected ConnectionIsClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
