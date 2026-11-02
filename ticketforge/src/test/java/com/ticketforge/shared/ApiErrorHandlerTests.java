package com.ticketforge.shared;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLTransientConnectionException;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.CannotCreateTransactionException;

class ApiErrorHandlerTests {

    /** Found under load: an exhausted pool while opening a transaction used to become a 500. */
    @Test
    void poolTimeoutWhileOpeningATransactionIsA503() {
        var e = new CannotCreateTransactionException("Could not open JDBC Connection for transaction",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available"));
        var response = new ApiErrorHandler().overloaded(e);
        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getHeaders().getFirst("Retry-After")).isEqualTo("1");
        assertThat(response.getBody().code()).isEqualTo("OVERLOADED");
    }
}
