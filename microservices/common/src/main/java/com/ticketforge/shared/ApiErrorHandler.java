package com.ticketforge.shared;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
class ApiErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiErrorHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ErrorResponse> api(ApiException e) {
        return ResponseEntity.status(e.status()).body(new ErrorResponse(e.code(), e.getMessage(), e.details()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorResponse> validation(MethodArgumentNotValidException e) {
        Map<String, Object> fields = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors().forEach(f -> fields.put(f.getField(), f.getDefaultMessage()));
        return ResponseEntity.badRequest().body(new ErrorResponse("VALIDATION_FAILED", "Invalid request", fields));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    ResponseEntity<ErrorResponse> unreadable(Exception e) {
        return ResponseEntity.badRequest().body(new ErrorResponse("MALFORMED_REQUEST", "Malformed request", Map.of()));
    }

    /**
     * Pool exhausted / DB unreachable / lock or statement timeout: tell clients to back off
     * instead of a 500. A pool timeout surfaces as CannotGetJdbcConnectionException from plain
     * JDBC calls but as CannotCreateTransactionException when a transaction is being opened.
     */
    @ExceptionHandler({CannotGetJdbcConnectionException.class, CannotCreateTransactionException.class,
            DataAccessResourceFailureException.class, TransientDataAccessException.class})
    ResponseEntity<ErrorResponse> overloaded(Exception e) {
        log.warn("Database overloaded: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "1")
                .body(new ErrorResponse("OVERLOADED", "Try again shortly", Map.of()));
    }

    /**
     * A database connection that dies mid-transaction (database stopped or restarted) does not always surface as
     * one of the exceptions above: found in the PostgreSQL failure experiment, three checkouts got a raw 500 from
     * "Connection is closed" and "terminating connection due to administrator command". Only genuine
     * connection-level failures are mapped to 503; any other error here is a bug and stays a 500.
     */
    @ExceptionHandler({TransactionSystemException.class, UncategorizedSQLException.class})
    ResponseEntity<ErrorResponse> maybeConnectionLoss(Exception e) throws Exception {
        if (isConnectionFailure(e)) {
            return overloaded(e);
        }
        throw e;
    }

    static boolean isConnectionFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql) {
                String state = sql.getSQLState();
                String message = sql.getMessage() == null ? "" : sql.getMessage();
                if ((state != null && (state.startsWith("08") || state.startsWith("57P")))
                        || message.contains("Connection is closed") || message.contains("terminating connection")) {
                    return true;
                }
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
