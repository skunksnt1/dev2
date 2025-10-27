package com.btg.pactual.hub.news;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;

/**
 * Destino de log/arquivo para MRN, com separação por idioma e geração de versões "limpas".
 */
public class FileQueueSink {

    private final Path logPath = Path.of("websocket_log.txt");
    private final Path baseDir = Path.of("mrn_noticias"); // raiz onde ficarão as subpastas por idioma
    private final ObjectMapper mapper = new ObjectMapper();

    public FileQueueSink() {
        try {
            if (Files.notExists(logPath)) Files.createFile(logPath);
            if (Files.notExists(baseDir)) Files.createDirectories(baseDir);
            appendLine("===== START at " + LocalDateTime.now() + " =====");
        } catch (IOException e) {
            throw new RuntimeException("Erro ao preparar diretórios/arquivos de log.", e);
        }
    }

    public synchronized void appendLine(String line) throws IOException {
        String ts = "[" + LocalDateTime.now() + "] " + line + System.lineSeparator();
        System.out.print(ts);
        Files.writeString(logPath, ts, StandardOpenOption.APPEND);
    }

    /**
     * Salva a notícia em:
     * - JSON original:   mrn_noticias/<language>/<guid>.json
     * - JSON tratado:    mrn_noticias/<language>/<guid>_clean.json
     *
     * O campo "body" é tratado para remover:
     * 1) ((Tradução automatizada... rtrsauto))
     * 2) (link)
     * 3) Quebras de linha (\n):
     *    - ".\n<LetraMaiúscula>" vira ".<br><LetraMaiúscula>"
     *    - Demais \n viram espaço simples
     */
    public synchronized void enqueueDecodedNews(String newsJson, String guid) {
        try {
            // 1) Descobrir idioma
            JsonNode root = mapper.readTree(newsJson);
            String lang = root.path("language").asText("unknown").toLowerCase().trim();
            if (lang.isEmpty()) lang = "unknown";

            // 2) Criar pasta
            Path langDir = baseDir.resolve(lang);
            Files.createDirectories(langDir);

            // 3) Nome base
            String safeGuid = (guid == null || guid.isBlank())
                    ? String.valueOf(System.currentTimeMillis())
                    : guid.replaceAll("[^A-Za-z0-9._-]", "_");

            // --- SALVAR JSON CRU ---
            Path rawTarget = uniquePath(langDir, safeGuid + ".json");
            Files.writeString(rawTarget, newsJson, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            appendLine("[NEWS-SAVED] (RAW)   lang=" + lang + " file=" + rawTarget.toAbsolutePath());

            // --- TRATAR BODY ---
            ObjectNode cleanRoot = (root.isObject()) ? (ObjectNode) root.deepCopy() : mapper.createObjectNode();
            String body = root.path("body").asText("");
            String cleanedBody = cleanBody(body);
            cleanRoot.put("body", cleanedBody);
            cleanRoot.put("_cleaned", true); // flag opcional

            String cleanJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cleanRoot);

            Path cleanTarget = uniquePath(langDir, safeGuid + "_clean.json");
            Files.writeString(cleanTarget, cleanJson, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            appendLine("[NEWS-SAVED] (CLEAN) lang=" + lang + " file=" + cleanTarget.toAbsolutePath());

        } catch (Exception e) {
            try {
                appendLine("[NEWS-SAVE-ERROR] " + e.getMessage());
            } catch (IOException ignored) {}
        }
    }

    /** Garante nome único (ex: file_1.json, file_2.json, etc.) */
    private Path uniquePath(Path dir, String fileName) {
        Path target = dir.resolve(fileName);
        if (!Files.exists(target)) return target;

        String base;
        String ext;
        int dot = fileName.lastIndexOf('.');
        if (dot >= 0) {
            base = fileName.substring(0, dot);
            ext = fileName.substring(dot);
        } else {
            base = fileName;
            ext = "";
        }

        int suffix = 1;
        Path candidate = target;
        while (Files.exists(candidate)) {
            candidate = dir.resolve(base + "_" + suffix + ext);
            suffix++;
        }
        return candidate;
    }

    /** Aplica as regras de limpeza no campo "body". */
    private String cleanBody(String body) {
        if (body == null) return "";

        String txt = body.replace("\r\n", "\n").replace("\r", "\n");

        // Remover aviso da Reuters (DOTALL -> atravessa quebras)
        txt = txt.replaceAll("\\(\\(Tradução automatizada[^)]*?rtrsauto\\)\\)", "");

        // Remover "(link)"
        txt = txt.replace("(link)", "");

        // ".\n" seguido de letra maiúscula vira ".<br>"
        txt = txt.replaceAll("\\.\\s*\\n\\s*([A-ZÁÂÃÀÉÊÍÓÔÕÚÜÇ])", ".<br>$1");

        // Outras quebras -> espaço
        txt = txt.replace("\n", " ");

        // Compactar espaços múltiplos
        txt = txt.replaceAll("[ \\t]{2,}", " ").trim();

        return txt;
    }
}
