package com.btg.pactual.hub.news;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Cliente para obter o Access Token RTO (V1 - Password Grant) via HTTP POST. */
public class RtoTokenClient {

    private static final String AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token";
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String clientId;
    private final String username;
    private final String password;
    private volatile String currentToken;

    public RtoTokenClient(String clientId, String username, String password) {
        this.clientId = clientId;
        this.username = username;
        this.password = password;
    }

    public String getUsername() { return username; }

    /** Obtém (e cacheia) o Access Token. */
    public synchronized String getAccessToken() throws IOException {
        if (currentToken != null && !currentToken.isBlank()) return currentToken;


        Map<String, String> data = new LinkedHashMap<>();
        data.put("grant_type", "password");
        data.put("username", username);
        data.put("password", password);
        data.put("client_id", clientId);
       // data.put("region", "us-east-1");
        data.put("takeExclusiveSignOnControl", "true");
        // *** ESCOPOS ***
        // Necessário para o Service Discovery (/streaming/pricing/v1/)
        data.put("scope", "trapi.streaming.pricing.read");
        // Se quiser já incluir news (para outros endpoints REST de news), você pode somar:
        // data.put("scope", "trapi.streaming.pricing.read trapi.streaming.news.read");

        String body = data.entrySet().stream()
                .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
                .collect(Collectors.joining("&"));

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(AUTH_URL))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        try {
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() != 200) {
                throw new IOException("Auth falhou: HTTP " + res.statusCode() + " body=" + res.body());
            }
            JsonNode root = mapper.readTree(res.body());
            String token = root.path("access_token").asText();
            if (token == null || token.isBlank()) {
                throw new IOException("access_token vazio na resposta: " + res.body());
            }
            currentToken = token;
            return currentToken;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Auth interrompida", ie);
        }
    }

    private static String enc(String v) {
        return URLEncoder.encode(v == null ? "" : v, StandardCharsets.UTF_8);
    }
}
