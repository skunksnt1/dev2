package com.btg.pactual.hub.news.builder;
import com.btg.pactual.hub.news.dto.NewsEventDTO;
import com.fasterxml.jackson.databind.*; import lombok.RequiredArgsConstructor; import org.springframework.stereotype.Component;
import java.time.Instant; import java.util.stream.StreamSupport;
@Component @RequiredArgsConstructor
public class NewsEventBuilder {
  private final ObjectMapper mapper;
  public NewsEventDTO fromMrn(String raw) {
    try {
      JsonNode n = mapper.readTree(raw);
      NewsEventDTO.NewsEventDTOBuilder b = NewsEventDTO.builder()
        .id(n.path("altId").asText())
        .language(n.path("language").asText("pt"))
        .headline(n.path("headline").asText(null))
        .body(n.path("body").asText(""))
        .receivedAt(Instant.now());
      if (n.path("audiences").isArray()) {
        java.util.List<String> a = new java.util.ArrayList<>();
        for (JsonNode it : n.path("audiences")) a.add(it.asText());
        b.audiences(a);
      }
      return b.build();
    } catch (Exception e) { throw new IllegalArgumentException("Payload MRN inválido", e); }
  }
}
