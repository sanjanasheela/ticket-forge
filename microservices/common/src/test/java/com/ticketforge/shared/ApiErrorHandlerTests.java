package com.ticketforge.shared;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

class ApiErrorHandlerTests {

    private final ApiErrorHandler handler = new ApiErrorHandler();

    /** Found under load in Phase 1: an exhausted pool while opening a transaction used to become a 500. */
    @Test
    void poolTimeoutWhileOpeningATransactionIsA503() {
        var e = new CannotCreateTransactionException("Could not open JDBC Connection for transaction",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available"));
        var response = handler.overloaded(e);
        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getHeaders().getFirst("Retry-After")).isEqualTo("1");
        assertThat(response.getBody().code()).isEqualTo("OVERLOADED");
    }

    /** Found in the Phase 2 PostgreSQL failure experiment: a connection dying during commit used to become a 500. */
    @Test
    void aConnectionClosedDuringCommitIsA503() throws Exception {
        var e = new TransactionSystemException("Could not commit JDBC transaction", new SQLException("Connection is closed"));
        assertThat(handler.maybeConnectionLoss(e).getStatusCode().value()).isEqualTo(503);
    }

    @Test
    void aServerSideTerminationIsA503() throws Exception {
        var e = new UncategorizedSQLException("task", "UPDATE x", new SQLException("FATAL: terminating connection due to administrator command", "57P01"));
        assertThat(handler.maybeConnectionLoss(e).getStatusCode().value()).isEqualTo(503);
    }

    /** The mapping must not hide bugs: a constraint violation or bad SQL is still an error. */
    @Test
    void anUnrelatedSqlErrorIsNotDisguisedAsOverload() {
        var e = new UncategorizedSQLException("task", "SELEKT", new SQLException("syntax error at or near \"SELEKT\"", "42601"));
        assertThatThrownBy(() -> handler.maybeConnectionLoss(e)).isSameAs(e);
    }
}
