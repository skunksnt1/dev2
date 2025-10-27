package com.btg.pactual.hub.news.dto;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import java.time.Instant;
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class NewsEventDTO {
  @NotBlank private String id;
  private String language; private String headline; private String body;
  private java.util.List<String> audiences;
  private SourceDTO source;
  private java.util.List<ImageDTO> images;
  private Instant receivedAt;
}
