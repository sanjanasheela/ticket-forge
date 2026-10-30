package com.ticketforge.catalog;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.catalog.CatalogDtos.EventSpec;
import com.ticketforge.catalog.CatalogDtos.EventView;
import com.ticketforge.catalog.CatalogDtos.PriceSpec;
import com.ticketforge.catalog.CatalogDtos.ShowSpec;
import com.ticketforge.catalog.CatalogDtos.VenueCreated;
import com.ticketforge.catalog.CatalogDtos.VenueSpec;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

/**
 * Public catalog reads (served from the Redis cache when possible) and admin writes. Writes
 * evict the affected cache keys after the transaction has committed, so a reader never repopulates
 * the cache with data that is about to change.
 */
@RestController
class CatalogController {

    private final CatalogAdminService admin;
    private final CachedCatalog reads;
    private final CatalogApi catalog;

    CatalogController(CatalogAdminService admin, CachedCatalog reads, CatalogApi catalog) {
        this.admin = admin;
        this.reads = reads;
        this.catalog = catalog;
    }

    // ---- public reads ------------------------------------------------------------------------

    @GetMapping(path = "/events", produces = MediaType.APPLICATION_JSON_VALUE)
    String events(@RequestParam(required = false) ShowCategory category) {
        return reads.events(category);
    }

    @GetMapping(path = "/events/{id}/shows", produces = MediaType.APPLICATION_JSON_VALUE)
    String shows(@PathVariable long id) {
        return reads.showsOfEvent(id);
    }

    @GetMapping(path = "/shows/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    String show(@PathVariable long id) {
        return reads.show(id);
    }

    // ---- admin writes ------------------------------------------------------------------------

    @PostMapping("/admin/venues")
    @ResponseStatus(HttpStatus.CREATED)
    VenueCreated createVenue(@Valid @RequestBody VenueSpec spec) {
        return admin.createVenue(spec);
    }

    @PostMapping("/admin/events")
    @ResponseStatus(HttpStatus.CREATED)
    EventView createEvent(@Valid @RequestBody EventSpec spec) {
        EventView created = admin.createEvent(spec);
        reads.eventCreated();
        return created;
    }

    @PostMapping("/admin/shows")
    @ResponseStatus(HttpStatus.CREATED)
    Map<String, Long> createShow(@Valid @RequestBody ShowSpec spec) {
        long id = admin.createShow(spec);
        reads.showChanged(id, spec.eventId());
        return Map.of("id", id);
    }

    @PutMapping("/admin/shows/{id}/prices")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void updatePrices(@PathVariable long id, @RequestBody @NotEmpty List<@Valid PriceSpec> prices) {
        admin.updatePrices(id, prices);
        reads.showChanged(id, catalog.eventIdOf(id).orElse(null));
    }
}
