package com.btg.pactual.hub.news.model;
import lombok.*; import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.*; import org.springframework.data.mongodb.core.mapping.Document;
import java.time.Instant;
@Document(collection = "news")
@CompoundIndexes({
  @CompoundIndex(name = "lang_received_idx", def = "{'language':1,'receivedAt':-1}"),
  @CompoundIndex(name = "text_idx", def = "{'headline':'text','body':'text'}")
})
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class NewsDocument {
  @Id public String mongoId;
  @Indexed(unique = true) public String id;
  public String language; public String headline; public String body;
  public java.util.List<String> audiences; public Source source; public java.util.List<Image> images;
  @Indexed(direction = IndexDirection.DESCENDING) public Instant receivedAt;
  public Instant expireAt;
  @Data @Builder @NoArgsConstructor @AllArgsConstructor public static class Image {
    public String url; public String caption; public Integer width; public Integer height; public String mimeType;
  }
  @Data @Builder @NoArgsConstructor @AllArgsConstructor public static class Source {
    public String provider; public String service; public String topic;
  }
}
