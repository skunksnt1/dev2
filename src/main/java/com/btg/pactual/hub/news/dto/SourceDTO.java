package com.btg.pactual.hub.news.dto;
import lombok.*; @Data @Builder @NoArgsConstructor @AllArgsConstructor
public class SourceDTO { public String provider; public String service; public String topic; }
