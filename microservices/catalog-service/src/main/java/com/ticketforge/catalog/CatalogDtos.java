package com.ticketforge.catalog;

import java.time.Instant;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

/** Request and response shapes of the catalog HTTP API. */
public final class CatalogDtos {

    private CatalogDtos() {
    }

    public enum TierKind { RESERVED, GA }

    // ---- venue layout ------------------------------------------------------------------------

    public record VenueSpec(@NotBlank String name, @NotBlank String city,
            @NotEmpty List<@Valid TierSpec> tiers, List<@Valid SectionSpec> sections) {
    }

    public record TierSpec(@NotBlank String name, @NotNull TierKind kind, @Positive Integer gaCapacity) {
    }

    public record SectionSpec(@NotBlank String name, @NotEmpty List<@Valid RowSpec> rows) {
    }

    /**
     * One or more identical rows. {@code labels} is "A-Z", "1-50", "A,B,C" or a single label.
     * {@code aisleAfter} lists seat numbers with an aisle to their right.
     */
    public record RowSpec(@NotBlank String labels, @NotBlank String tier, @Min(1) @Max(500) int seats, List<Integer> aisleAfter) {
    }

    public record TierView(long id, String name, String kind, Integer gaCapacity) {
    }

    public record VenueCreated(long id, String name, List<TierView> tiers, int seatCount) {
    }

    // ---- events and shows --------------------------------------------------------------------

    public record EventSpec(@NotBlank String title, @NotBlank String genre, @Positive int durationMin, @NotNull ShowCategory category) {
    }

    public record EventView(long id, String title, String genre, int durationMin, ShowCategory category) {
    }

    public record PriceSpec(@NotNull Long tierId, @PositiveOrZero long priceMinor) {
    }

    /** Null rule fields fall back to the event category's defaults. maxHoldsPerUser 0 = unlimited. */
    public record ShowSpec(@NotNull Long eventId, @NotNull Long venueId, @NotNull Instant startsAt, Instant saleOpensAt,
            String currency, @NotEmpty List<@Valid PriceSpec> prices,
            @Positive Integer holdTtlSec, Boolean queueEnabled, @Positive Integer maxSeatsPerHold,
            @PositiveOrZero Integer maxHoldsPerUser, @Positive Integer admitRatePerSec,
            @Min(0) @Max(64) Integer gaShards) {
    }

    public record ShowSummary(long id, long eventId, String eventTitle, ShowCategory category, long venueId,
            String venueName, String city, Instant startsAt, Instant saleOpensAt) {
    }

    public record ShowTier(long id, String name, String kind, Integer gaCapacity, long priceMinor) {
    }

    public record ShowDetails(ShowSummary show, String currency, int holdTtlSec, boolean queueEnabled,
            int maxSeatsPerHold, Integer maxHoldsPerUser, int gaShards, List<ShowTier> tiers) {
    }
}
