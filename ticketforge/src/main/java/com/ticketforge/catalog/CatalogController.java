package com.ticketforge.catalog;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
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
import com.ticketforge.catalog.CatalogDtos.ShowDetails;
import com.ticketforge.catalog.CatalogDtos.ShowSpec;
import com.ticketforge.catalog.CatalogDtos.ShowSummary;
import com.ticketforge.catalog.CatalogDtos.VenueCreated;
import com.ticketforge.catalog.CatalogDtos.VenueSpec;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

@RestController
class CatalogController {

    private final CatalogAdminService admin;
    private final CatalogQueries queries;

    CatalogController(CatalogAdminService admin, CatalogQueries queries) {
        this.admin = admin;
        this.queries = queries;
    }

    // ---- public reads ------------------------------------------------------------------------

    @GetMapping("/events")
    List<EventView> events(@RequestParam(required = false) ShowCategory category) {
        return queries.events(category);
    }

    @GetMapping("/events/{id}/shows")
    List<ShowSummary> shows(@PathVariable long id) {
        return queries.showsOfEvent(id);
    }

    @GetMapping("/shows/{id}")
    ShowDetails show(@PathVariable long id) {
        return queries.show(id);
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
        return admin.createEvent(spec);
    }

    @PostMapping("/admin/shows")
    @ResponseStatus(HttpStatus.CREATED)
    Map<String, Long> createShow(@Valid @RequestBody ShowSpec spec) {
        return Map.of("id", admin.createShow(spec));
    }

    @PutMapping("/admin/shows/{id}/prices")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void updatePrices(@PathVariable long id, @RequestBody @NotEmpty List<@Valid PriceSpec> prices) {
        admin.updatePrices(id, prices);
    }
}
