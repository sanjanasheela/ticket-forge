package com.ticketforge.auth;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@RestController
@RequestMapping("/auth")
class AuthController {

    private final AuthService auth;

    AuthController(AuthService auth) {
        this.auth = auth;
    }

    @PostMapping("/signup")
    @ResponseStatus(HttpStatus.CREATED)
    AuthService.Tokens signup(@Valid @RequestBody Credentials body) {
        return auth.signup(body.email(), body.password());
    }

    @PostMapping("/login")
    AuthService.Tokens login(@Valid @RequestBody Credentials body) {
        return auth.login(body.email(), body.password());
    }

    @PostMapping("/refresh")
    AuthService.Tokens refresh(@Valid @RequestBody RefreshRequest body) {
        return auth.refresh(body.refreshToken());
    }

    @PostMapping("/logout")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void logout(@Valid @RequestBody RefreshRequest body) {
        auth.logout(body.refreshToken());
    }

    record Credentials(@NotBlank @Email String email, @NotBlank @Size(min = 8, max = 72) String password) {
    }

    record RefreshRequest(@NotBlank String refreshToken) {
    }
}
