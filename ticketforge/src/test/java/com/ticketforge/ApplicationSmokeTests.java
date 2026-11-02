package com.ticketforge;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ApplicationSmokeTests extends IntegrationTest {

    @Test
    void startsAgainstRealPostgresAndReportsHealthy() {
        assertThat(mvc.get().uri("/actuator/health"))
                .hasStatusOk()
                .bodyJson().extractingPath("$.status").isEqualTo("UP");
    }

    @Test
    void exposesPrometheusMetrics() {
        assertThat(mvc.get().uri("/actuator/prometheus"))
                .hasStatusOk()
                .bodyText().contains("hikaricp_connections_max");
    }
}
