package com.ticketforge.catalog.seed;

import java.util.Map;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Same endpoints and parameters as Phase 1, so the k6 scripts run unchanged. Admin only. */
@RestController
@RequestMapping("/admin/seed")
class SeedController {

    private final SeedService seed;

    SeedController(SeedService seed) {
        this.seed = seed;
    }

    @PostMapping("/demo")
    SeedService.DemoResult demo() {
        return seed.demo();
    }

    @PostMapping("/concert-show")
    Map<String, Long> concertShow(@RequestParam(required = false) Boolean queue,
            @RequestParam(required = false) Integer holdTtlSec,
            @RequestParam(required = false) Integer maxHoldsPerUser,
            @RequestParam(required = false) Integer saleOpensInSec,
            @RequestParam(defaultValue = "0") int gaShards) {
        return Map.of("showId", seed.concertShow(
                new SeedService.ConcertOptions(queue, holdTtlSec, maxHoldsPerUser, saleOpensInSec, gaShards)));
    }

    @PostMapping("/movie-shows")
    Map<String, Object> movieShows(@RequestParam(defaultValue = "20") int count) {
        return Map.of("showIds", seed.movieShows(Math.min(count, 1000)));
    }
}
