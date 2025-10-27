package com.btg.pactual.hub.news.service;
import com.btg.pactual.hub.news.builder.NewsEventBuilder;
import com.btg.pactual.hub.news.constants.AppConstants;
import com.btg.pactual.hub.news.dto.NewsEventDTO;
import com.btg.pactual.hub.news.model.NewsDocument;
import com.btg.pactual.hub.news.repository.NewsRepository;
import lombok.RequiredArgsConstructor; import org.springframework.stereotype.Service;
import java.io.*; @Service @RequiredArgsConstructor
public class NewsRoutingService {
  private final NewsRepository repository; private final NewsEventBuilder builder;
  public void processIncomingJson(String rawJson) {
    if (AppConstants.SAVE_TO_FILE) saveToFile(rawJson); else saveToMongo(rawJson);
  }
  private void saveToFile(String rawJson) {
    try {
      java.io.File dir = new java.io.File(AppConstants.SAVE_PATH); if (!dir.exists()) dir.mkdirs();
      String fileName = AppConstants.SAVE_PATH + "news_" + System.currentTimeMillis() + ".json";
      try (java.io.FileWriter fw = new java.io.FileWriter(fileName)) { fw.write(rawJson); }
      System.out.println("[SAVE_TO_FILE] " + fileName);
    } catch (IOException e) { System.err.println("Erro ao salvar arquivo: " + e.getMessage()); }
  }
  private void saveToMongo(String rawJson) {
    try {
      NewsEventDTO dto = builder.fromMrn(rawJson);
      NewsDocument doc = NewsDocument.builder()
        .id(dto.getId()).language(dto.getLanguage()).headline(dto.getHeadline())
        .body(dto.getBody()).audiences(dto.getAudiences()).receivedAt(dto.getReceivedAt())
        .build();
      repository.save(doc);
      System.out.println("[SAVE_TO_MONGO] " + dto.getId());
    } catch (Exception e) { System.err.println("Erro ao salvar no Mongo: " + e.getMessage()); }
  }
}
