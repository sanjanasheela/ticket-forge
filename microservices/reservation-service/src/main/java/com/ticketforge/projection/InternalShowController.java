package com.ticketforge.projection;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.shared.ApiException;

/** 200 once a show's inventory exists, 404 before. Lets catalog's seeders wait for the asynchronous build. */
@RestController
class InternalShowController {

    private final ShowProjection projection;

    InternalShowController(ShowProjection projection) {
        this.projection = projection;
    }

    @GetMapping("/internal/shows/{id}/ready")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void ready(@PathVariable long id) {
        if (!projection.inventoryReady(id)) {
            throw ApiException.notFound("INVENTORY_NOT_READY", "Inventory for show " + id + " is not built yet");
        }
    }
}
