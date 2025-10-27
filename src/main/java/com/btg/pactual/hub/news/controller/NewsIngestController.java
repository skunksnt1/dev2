package com.btg.pactual.hub.news.controller;
import com.btg.pactual.hub.news.service.NewsRoutingService; import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity; import org.springframework.web.bind.annotation.*;
@RestController @RequestMapping("/api/news") @RequiredArgsConstructor
public class NewsIngestController {
  private final NewsRoutingService routing;
  @PostMapping("/test") public ResponseEntity<String> ingest(@RequestBody String rawJson) {
    routing.processIncomingJson(rawJson); return ResponseEntity.ok("OK");
  }
}
