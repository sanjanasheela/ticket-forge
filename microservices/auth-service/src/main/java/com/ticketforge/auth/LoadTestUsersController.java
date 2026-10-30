package com.ticketforge.auth;

import java.util.Map;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Creates the fixed-id users the k6 scripts mint JWTs for. Dev and load testing only. */
@RestController
@RequestMapping("/admin/seed")
class LoadTestUsersController {

    private final UserProvisioning users;

    LoadTestUsersController(UserProvisioning users) {
        this.users = users;
    }

    @PostMapping("/loadtest-users")
    Map<String, Integer> loadTestUsers(@RequestParam(defaultValue = "1000") int count) {
        return Map.of("count", users.ensureLoadTestUsers(Math.min(count, 500_000)));
    }
}
