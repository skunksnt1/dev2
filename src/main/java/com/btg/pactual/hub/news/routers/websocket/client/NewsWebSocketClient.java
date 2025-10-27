package com.btg.pactual.hub.news.routers.websocket.client;

import com.btg.pactual.hub.news.FileQueueSink;
import com.btg.pactual.hub.news.MrnFragmentReassembler;
import com.btg.pactual.hub.news.RtoTokenClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.Base64;

public class NewsWebSocketClient implements WebSocket.Listener, AutoCloseable {

    private volatile WebSocket socket;
    private volatile boolean closed = false;
    private volatile boolean loginOpen = false;

    // Descoberta + endpoints ativos (SEM default)
    private volatile List<String> wsEndpoints = List.of();
    private volatile int endpointIdx = 0;
    private final Object endpointsLock = new Object();

    // Reagrupador de fragments
    private final MrnFragmentReassembler reassembler = new MrnFragmentReassembler(60);

    // Contador e chave temporária para mensagens sem GUID
    private final AtomicInteger unknownCounter = new AtomicInteger(0);
    private volatile String lastUnknownKey = null;

    // --- CREDENCIAIS ---
    private static final String CLIENT_ID = System.getenv().getOrDefault("RTO_CLIENTID", "6ad45100841f4a96951f6583e4ba022d57a6565f");
    private static final String USERNAME  = System.getenv().getOrDefault("RTO_USERNAME", "GE-A-00209895-3-19960");
    private static final String PASSWORD  = System.getenv().getOrDefault("RTO_PASSWORD", "senh@-content-hub2025-research-2025@#");
    private static final String APP_ID    = System.getenv().getOrDefault("APP_ID", "256");
    private static final String SERVICE   = "ELEKTRON_DD";
    private static final String NEWS_RIC  = "MRN_STORY";

    // REST discovery endpoints (iguais ao seu log)
    private static final String AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token";
    private static final String DISCOVERY_URL = "https://api.refinitiv.com/streaming/pricing/v1/";

    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final int reconnectSeconds;
    private final RtoTokenClient tokenClient;
    private final FileQueueSink sink;

    public NewsWebSocketClient(List<String> bootstrapEndpoints,
                               int reconnectSeconds,
                               RtoTokenClient tokenClient,
                               FileQueueSink sink) {
        this.reconnectSeconds = reconnectSeconds;
        this.tokenClient = tokenClient;
        this.sink = sink;
        // pode vir vazio; se vier, faremos discovery antes de conectar
        updateEndpoints(bootstrapEndpoints == null ? List.of() : bootstrapEndpoints, false);
    }

    public NewsWebSocketClient(String endpointsCsvOrSingle,
                               int reconnectSeconds,
                               RtoTokenClient tokenClient,
                               FileQueueSink sink) {
        this(parseEndpointsFromCsvOrSingle(endpointsCsvOrSingle), reconnectSeconds, tokenClient, sink);
    }

    // ====== LOG helpers ======
    private void logOut(String line) {
        try { sink.appendLine(line); } catch (Exception ignore) {}
        System.out.println(line);
    }

    private void logSentPayload(String label, String payload) {
        try (FileWriter fw = new FileWriter("ws_payloads.log", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            String ts = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            out.println("==== " + label + " @ " + ts + " ====");
            out.println(payload);
            out.println("==== END ====\n");
        } catch (IOException e) { e.printStackTrace(); }
        // espelha no console no formato do seu exemplo
        logOut("SENT:\n" + pretty(payload));
    }

    private String pretty(String json) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(json));
        } catch (Exception e) {
            return json;
        }
    }

    private static List<String> parseEndpointsFromCsvOrSingle(String csvOrSingle) {
        if (csvOrSingle == null || csvOrSingle.isBlank()) return List.of();
        String[] parts = csvOrSingle.split(",");
        List<String> list = new ArrayList<>();
        for (String p : parts) {
            String s = p.trim();
            if (!s.isEmpty()) list.add(s);
        }
        return list;
    }

    private void updateEndpoints(List<String> newEndpoints, boolean reconnectNow) {
        List<String> norm = new ArrayList<>();
        if (newEndpoints != null) {
            for (String e : newEndpoints) {
                if (e != null && !e.trim().isEmpty()) norm.add(e.trim());
            }
        }
        synchronized (endpointsLock) {
            this.wsEndpoints = Collections.unmodifiableList(norm);
            if (!wsEndpoints.isEmpty() && endpointIdx >= wsEndpoints.size()) endpointIdx = 0;
        }
        logOut("[ENDPOINTS] " + (wsEndpoints.isEmpty() ? "EMPTY" : wsEndpoints.toString()));
        if (reconnectNow) {
            safeAbort();
            scheduleReconnect();
        }
    }

    // ====== SERVICE DISCOVERY ======
    private void ensureEndpointsViaDiscovery() {
        // se já tem endpoints de bootstrap, usa; senão, descobre
        List<String> snapshot;
        synchronized (endpointsLock) { snapshot = wsEndpoints; }
        if (!snapshot.isEmpty()) return;

        try {
            logOut("Sending authentication request with password to " + AUTH_URL + " ...");
            String token = tokenClient.getAccessToken(); // já faz o POST/auth internamente
            // apenas para espelhar o log: mostre o token JSON “RECEIVED”
            // se seu RtoTokenClient expõe o payload, você pode logar aqui; caso não, montamos o mínimo:
            Map<String, Object> authEcho = new LinkedHashMap<>();
            authEcho.put("access_token", token);
            authEcho.put("token_type", "Bearer");
            authEcho.put("scope", "trapi.streaming.pricing.read");
            logOut("Refinitiv Data Platform Authentication succeeded. RECEIVED:\n" +
                    pretty(mapper.writeValueAsString(authEcho)));

            logOut("Sending Refinitiv Data Platform service discovery request to " + DISCOVERY_URL + " ...");

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(DISCOVERY_URL))
                    .timeout(Duration.ofSeconds(20))
                    .header("Authorization", "Bearer " + token)
                    .header("Accept", "application/json")
                    .GET().build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                logOut("[DISCOVERY] HTTP " + resp.statusCode() + " body=" + resp.body());
                return;
            }
            String body = resp.body();
            logOut("Refinitiv Data Platform Service discovery succeeded. RECEIVED:\n" + pretty(body));

            // parse services[]
            JsonNode root = mapper.readTree(body);
            JsonNode services = root.path("services");
            List<String> discovered = new ArrayList<>();
            if (services.isArray()) {
                for (JsonNode s : services) {
                    String transport = s.path("transport").asText("");
                    if (!"websocket".equalsIgnoreCase(transport)) continue;

                    boolean okFormat = false;
                    JsonNode dformat = s.path("dataFormat");
                    if (dformat.isArray()) {
                        for (JsonNode df : dformat) {
                            if ("tr_json2".equalsIgnoreCase(df.asText(""))) { okFormat = true; break; }
                        }
                    }
                    if (!okFormat) continue;

                    String endpoint = s.path("endpoint").asText("");
                    int port = s.path("port").asInt(443);
                    if (!endpoint.isBlank()) {
                        String url = "wss://" + endpoint + ":" + port + "/WebSocket";
                        discovered.add(url);
                    }
                }
            }
            if (!discovered.isEmpty()) {
                updateEndpoints(discovered, false);
            }
        } catch (Exception e) {
            logOut("[DISCOVERY-ERROR] " + e.getMessage());
        }
    }

    // ====== CONEXÃO ======
    public void connect() {
        if (closed) return;

        // se necessário, faz discovery primeiro
        ensureEndpointsViaDiscovery();

        List<String> snapshot;
        synchronized (endpointsLock) { snapshot = this.wsEndpoints; }
        if (snapshot.isEmpty()) {
            logOut("ERRO: informe endpoints iniciais (ou deixe o discovery popular).");
            return;
        }

        int tried = 0;
        while (!closed && tried < snapshot.size()) {
            String endpoint;
            int idxNow;
            synchronized (endpointsLock) {
                endpoint = wsEndpoints.get(endpointIdx);
                idxNow = endpointIdx;
            }
            try {
                logOut("Connecting to WebSocket " + endpoint + " ...");

                WebSocket.Builder b = http.newWebSocketBuilder()
                        .connectTimeout(Duration.ofSeconds(20))
                        .subprotocols("tr_json2");

                this.socket = b.buildAsync(URI.create(endpoint), this).join();
                logOut("WebSocket successfully connected!");
                return;
            } catch (java.util.concurrent.CompletionException e) {
                logOut("[HANDSHAKE/ERROR] endpoint=" + endpoint + " cause=" + (e.getCause()==null?e:e.getCause()));
                rotateEndpoint();
                tried++;
            } catch (Exception e) {
                logOut("[ERROR] endpoint=" + endpoint + " ex=" + e.getMessage());
                rotateEndpoint();
                tried++;
            }
        }

        scheduleReconnect();
    }

    private void rotateEndpoint() {
        synchronized (endpointsLock) {
            if (!wsEndpoints.isEmpty()) endpointIdx = (endpointIdx + 1) % wsEndpoints.size();
        }
    }

    private void scheduleReconnect() {
        if (closed) return;
        try { Thread.sleep(reconnectSeconds * 1000L); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        connect();
    }

    private void safeAbort() {
        if (this.socket != null) { try { this.socket.abort(); } catch (Exception ignore) {} }
    }

    // ====== LISTENER ======
    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
        try {
            String token = tokenClient.getAccessToken();
            String loginJson = "{"
                    + "\"Domain\":\"Login\","
                    + "\"ID\":1,"
                    + "\"Key\":{"
                    + "\"Product codes\":[\"NP:BRS\"],"
                    + "\"Elements\":{"
                    + "\"ApplicationId\":\"" + APP_ID + "\","
                    + "\"AuthenticationToken\":\"" + token + "\","
                    + "\"Position\":\"127.0.0.1/net\""
                    + "},"
                    + "\"Name\":\"" + USERNAME + "\","
                    + "\"NameType\":\"AuthnToken\""
                    + "},"
                    + "\"Refresh\":false"
                    + "}";
            webSocket.sendText(loginJson, true);
            logSentPayload("LOGIN", loginJson);
        } catch (Exception ex) {
            logOut("[ERROR] Auth/Login falhou: " + ex.getMessage());
            safeAbort();
            scheduleReconnect();
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        String msg = data.toString();
        logOut("RECEIVED: " + (msg.isBlank() ? "" : "\n" + pretty(msg)));
        try {
            processIncomingMessage(msg);
        } catch (Exception e) {
            logOut("[PROCESS-ERROR] " + e.getMessage());
        }
        webSocket.request(1);
        return null;
    }

    private void processIncomingMessage(String message) throws IOException {
        JsonNode root = mapper.readTree(message);
        if (root.isArray()) {
            for (JsonNode m : root) processSingleMessage(m);
        } else {
            processSingleMessage(root);
        }
    }

    private void processSingleMessage(JsonNode msg) throws IOException {
        String domain = msg.path("Domain").asText();
        String type   = msg.path("Type").asText();

        if ("Ping".equals(type)) {
            String pong = "{\"Type\":\"Pong\"}";
            socket.sendText(pong, true);
            logSentPayload("Pong", pong);
            return;
        }

        if ("Login".equalsIgnoreCase(domain)) {
            if ("Refresh".equals(type)) {
                JsonNode st = msg.path("State");
                boolean ok = "Ok".equalsIgnoreCase(st.path("Data").asText())
                        && "Open".equalsIgnoreCase(st.path("Stream").asText());
                if (ok) {
                    loginOpen = true;
                    sendSubscribe();
                } else {
                    logOut("[LOGIN] Refresh não-OK: " + st);
                }
            } else if ("Status".equals(type)) {
                String code = msg.path("State").path("Code").asText("");
                if (code.contains("UserAccessToAppDenied")) {
                    safeAbort();
                    scheduleReconnect();
                }
            }
            return;
        }

        if ("NewsTextAnalytics".equalsIgnoreCase(domain)) {
            if (!loginOpen) { logOut("[WARN] Item antes do login abrir"); return; }
            if ("Refresh".equals(type) || "Update".equals(type)) processMrnFragment(msg);
            else if ("Status".equals(type)) logOut("[STATUS] " + msg);
        }
    }

    private void sendSubscribe() {
        String sub = "{"
                + "\"Domain\":\"NewsTextAnalytics\","
                + "\"ID\":2,"
                + "\"Key\":{"
                + "\"Name\":\"" + NEWS_RIC + "\","
                + "\"audiences\":[\"NP:BRS\"],"
                + "\"Product codes\":[\"NP:BRS\"],"
                //       + "\"Service\":\"" + SERVICE + "\","
                + "\"audiences\":[\"NP:BRS\"]"
                + "},"
                + "\"Topic\":[\"BR\"],"
                + "\"audiences\":[\"NP:BRS\"],"
                + "\"Language\":\"pt\""
                + "}";
        socket.sendText(sub, true);
        logSentPayload("SUBSCRIBE", sub);
    }

    // ====== MRN ======
    private void processMrnFragment(JsonNode msg) {
        JsonNode fields = msg.path("Fields");
        if (fields.isMissingNode()) return;

        JsonNode fragment = fields.path("FRAGMENT");
        JsonNode fragNum = fields.path("FRAG_NUM");
        JsonNode acumNode = fields.path("ACUM");
        JsonNode guidNode = fields.path("GUID");

        if (fragment.isMissingNode()) {
            String guid = guidNode.isMissingNode() ? null : guidNode.asText(null);
            Optional<String> assembled = reassembler.forceAssemble(guid);
            if (assembled.isPresent()) {
                try { sink.enqueueDecodedNews(decodePayloadFromString(assembled.get()), guid); }
                catch (Exception ex) { debugDecodeError(assembled.get(), "[MRN-DECODE-ERROR]"); }
            }
            return;
        }

        int frag = fragNum.isMissingNode() ? 1 : fragNum.asInt(1);
        int acum = acumNode.isMissingNode() ? 0 : acumNode.asInt(0);
        String guid = guidNode.isMissingNode() ? null : guidNode.asText(null);

        if (guid == null) {
            if (frag == 1) { lastUnknownKey = "UNKN-" + unknownCounter.incrementAndGet(); guid = lastUnknownKey; }
            else { guid = (lastUnknownKey == null) ? ("UNKN-" + unknownCounter.incrementAndGet()) : lastUnknownKey; }
        }

        Optional<String> assembledBase64 = reassembler.addFragment(guid, frag, acum, fragment.asText());
        if (assembledBase64.isPresent()) {
            String assembledB64 = assembledBase64.get();
            try { sink.enqueueDecodedNews(decodePayloadFromString(assembledB64), guid); }
            catch (Exception ex) { debugDecodeError(assembledB64, "[MRN-DECODE-ERROR]"); }
            finally { if (guid.startsWith("UNKN-")) lastUnknownKey = null; }
            return;
        }

        if (acum <= 1) {
            try { sink.enqueueDecodedNews(decodePayloadFromString(fragment.asText()), guid); }
            catch (Exception ex) { debugDecodeError(fragment.asText(), "[MRN-DECODE-ERROR-FORCE]"); }
            finally { if (guid.startsWith("UNKN-")) lastUnknownKey = null; }
        }
    }

    private void debugDecodeError(String base64OrRaw, String tag) {
        try {
            String preview = base64OrRaw.length() > 200 ? base64OrRaw.substring(0,200) + "..." : base64OrRaw;
            String hex = "";
            try {
                byte[] raw = Base64.getDecoder().decode(base64OrRaw);
                int len = Math.min(8, raw.length);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < len; i++) sb.append(String.format("%02X ", raw[i]));
                hex = sb.toString().trim();
            } catch (Exception ignore) {}
            logOut(tag + " preview_base64=" + preview);
            logOut(tag + " first_bytes_hex=" + hex);
        } catch (Exception ignore) {}
    }

    private String decodePayloadFromString(String base64OrRaw) throws IOException {
        if (base64OrRaw == null) return "";
        try { return decodeMaybeGzip(Base64.getDecoder().decode(base64OrRaw)); }
        catch (IllegalArgumentException iae) { return base64OrRaw; }
    }

    private String decodeMaybeGzip(byte[] compressed) throws IOException {
        if (compressed == null) return "";
        if (compressed.length >= 2 && (compressed[0] == (byte)0x1f && compressed[1] == (byte)0x8b)) {
            try (ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
                 GZIPInputStream gin = new GZIPInputStream(bin);
                 InputStreamReader r = new InputStreamReader(gin, StandardCharsets.UTF_8);
                 BufferedReader br = new BufferedReader(r)) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) sb.append(line);
                return sb.toString();
            }
        }
        return new String(compressed, StandardCharsets.UTF_8);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        logOut("[CLOSE] " + statusCode + ": " + reason);
        scheduleReconnect();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        logOut("[ERROR] " + (error.getMessage() == null ? error.toString() : error.getMessage()));
        scheduleReconnect();
    }

    @Override
    public void close() {
        this.closed = true;
        safeAbort();
    }

    // ====== MAIN ======
    public static void main(String[] args) {
        // opcional: CSV por args/env; se vazio, o client fará discovery antes de conectar
        String csv = System.getenv().getOrDefault("RDP_WS_ENDPOINTS", "");
        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) csv = args[0];
        List<String> bootstrap = parseEndpointsFromCsvOrSingle(csv);

        try (NewsWebSocketClient client = new NewsWebSocketClient(
                bootstrap, 30,
                new RtoTokenClient(CLIENT_ID, USERNAME, PASSWORD),
                new FileQueueSink())) {
            client.connect();
            new CompletableFuture<Void>().get();
        } catch (Exception e) {
            System.err.println("CRITICAL FAILURE: " + e.getMessage());
        }
    }
}
