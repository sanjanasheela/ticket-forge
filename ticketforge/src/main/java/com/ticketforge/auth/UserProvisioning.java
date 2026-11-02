package com.ticketforge.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/** Creates the bootstrap admin on startup, and bulk users for load tests. */
@Service
public class UserProvisioning implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(UserProvisioning.class);

    /** Password of every load-test user. They normally skip login: k6 mints their JWTs itself. */
    public static final String LOADTEST_PASSWORD = "loadtest-password";

    private final JdbcClient jdbc;
    private final JdbcTemplate template;
    private final PasswordEncoder passwords;
    private final AuthProperties props;

    UserProvisioning(JdbcClient jdbc, JdbcTemplate template, PasswordEncoder passwords, AuthProperties props) {
        this.jdbc = jdbc;
        this.template = template;
        this.passwords = passwords;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        var admin = props.bootstrapAdmin();
        int created = jdbc.sql("""
                        INSERT INTO users (email, password_hash, role) VALUES (:email, :hash, 'ADMIN')
                        ON CONFLICT ((lower(email))) DO NOTHING""")
                .param("email", admin.email())
                .param("hash", passwords.encode(admin.password()))
                .update();
        if (created > 0) {
            log.info("Bootstrap admin created email={}", admin.email());
        }
    }

    /**
     * Load-test user {@code i} has the deterministic id {@code 00000000-0000-4000-8000-<i as 12 digits>}
     * and email {@code loadtest-<i>@ticketforge.local}, so k6 can address users without a lookup.
     */
    public static UUID loadTestUserId(int i) {
        return UUID.fromString("00000000-0000-4000-8000-%012d".formatted(i));
    }

    public int ensureLoadTestUsers(int count) {
        String hash = passwords.encode(LOADTEST_PASSWORD);
        List<Object[]> rows = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            rows.add(new Object[] {loadTestUserId(i), "loadtest-" + i + "@ticketforge.local", hash});
        }
        int[][] results = template.batchUpdate(
                "INSERT INTO users (id, email, password_hash, role) VALUES (?, ?, ?, 'USER') ON CONFLICT DO NOTHING",
                rows, 1000, (ps, row) -> {
                    ps.setObject(1, row[0]);
                    ps.setString(2, (String) row[1]);
                    ps.setString(3, (String) row[2]);
                });
        int inserted = 0;
        for (int[] batch : results) {
            for (int r : batch) {
                inserted += Math.max(r, 0);
            }
        }
        log.info("Load-test users ensured count={} inserted={}", count, inserted);
        return count;
    }
}
