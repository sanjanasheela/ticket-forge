package com.ticketforge.security;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Service;

/**
 * Issues the two kinds of JWT the app uses. Both are HS256 with the same key, so each carries a
 * {@code typ} claim and each decoder only accepts its own type: a waiting-room token can never be
 * used as a login.
 */
@Service
public class TokenService {

    public static final String ISSUER = "ticketforge";
    public static final String TYPE_ACCESS = "access";
    public static final String TYPE_QUEUE = "queue";

    private final JwtEncoder encoder;
    private final JwtDecoder queueDecoder;
    private final Clock clock;
    private final Duration accessTtl;

    public TokenService(JwtEncoder encoder, JwtConfig.QueueJwtDecoder queueDecoder, Clock clock, TokenProperties props) {
        this.encoder = encoder;
        this.queueDecoder = queueDecoder.delegate();
        this.clock = clock;
        this.accessTtl = props.accessTokenTtl();
    }

    public String issueAccess(UUID userId, String role) {
        Instant now = clock.instant();
        return encode(JwtClaimsSet.builder()
                .issuer(ISSUER)
                .subject(userId.toString())
                .issuedAt(now)
                .expiresAt(now.plus(accessTtl))
                .claim("typ", TYPE_ACCESS)
                .claim("roles", List.of(role))
                .build());
    }

    public long accessTtlSeconds() {
        return accessTtl.toSeconds();
    }

    /** Proof that {@code userId} got through the waiting room for {@code showId}. */
    public QueueAccess issueQueueAccess(UUID userId, long showId, Duration ttl) {
        Instant now = clock.instant();
        Instant expires = now.plus(ttl);
        String token = encode(JwtClaimsSet.builder()
                .issuer(ISSUER)
                .subject(userId.toString())
                .issuedAt(now)
                .expiresAt(expires)
                .claim("typ", TYPE_QUEUE)
                .claim("show", showId)
                .build());
        return new QueueAccess(token, expires);
    }

    /** True only for an unexpired queue token issued to this user for this show. */
    public boolean isValidQueueAccess(String token, UUID userId, long showId) {
        try {
            Jwt jwt = queueDecoder.decode(token);
            Object show = jwt.getClaims().get("show");
            return userId.toString().equals(jwt.getSubject())
                    && show instanceof Number n && n.longValue() == showId;
        } catch (JwtException e) {
            return false;
        }
    }

    private String encode(JwtClaimsSet claims) {
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    public record QueueAccess(String token, Instant expiresAt) {
    }
}
