package com.btg.pactual.hub.news.dto;
import lombok.*; @Data @Builder @NoArgsConstructor @AllArgsConstructor
public class ImageDTO { public String url; public String caption; public Integer width; public Integer height; public String mimeType; }
