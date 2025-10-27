package com.btg.pactual.hub.news;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Reagrupa fragments MRN por GUID (quando disponível) ou por chave fallback.
 * Thread-safe.
 */
public class MrnFragmentReassembler {

    private static final String UNKNOWN_KEY = "UNKN";
    private final ConcurrentMap<String, FragmentState> states = new ConcurrentHashMap<>();
    private final long timeoutSeconds;

    public MrnFragmentReassembler(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * Adiciona um fragmento e, se todos os pedaços estiverem presentes, retorna o payload base64 montado.
     * Se total<=0 e fragNum==1 tratamos como mensagem única (fallback prático quando ACUM não é enviado).
     */
    public Optional<String> addFragment(String guid, int fragNum, int total, String payload) {
        String key = keyFor(guid);
        cleanupStale();
        FragmentState state = states.computeIfAbsent(key, k -> new FragmentState(total));
        synchronized (state) {
            if (total > 0) state.expectedTotal = total;
            state.parts.put(fragNum, payload);
            state.lastUpdate = Instant.now().getEpochSecond();

            // Fallback: se total não informado e é o primeiro fragmento, monta imediatamente
            if ((state.expectedTotal <= 0) && fragNum == 1) {
                // remove estado e retorna o payload atual
                states.remove(key);
                return Optional.of(payload);
            }

            if (state.expectedTotal > 0 && state.parts.size() >= state.expectedTotal) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= state.expectedTotal; i++) {
                    String part = state.parts.get(i);
                    if (part != null) sb.append(part);
                }
                states.remove(key);
                return Optional.of(sb.toString());
            }
        }
        return Optional.empty();
    }

    /**
     * Força a montagem dos fragments já recebidos para a chave (guid ou fallback).
     * Retorna Optional com base64 montado se houver partes.
     */
    public Optional<String> forceAssemble(String guid) {
        String key = keyFor(guid);
        FragmentState state = states.remove(key);
        if (state == null) return Optional.empty();
        synchronized (state) {
            if (state.parts.isEmpty()) return Optional.empty();
            int max = state.parts.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= max; i++) {
                String part = state.parts.get(i);
                if (part != null) sb.append(part);
            }
            return Optional.of(sb.toString());
        }
    }

    private String keyFor(String guid) {
        if (guid == null || guid.isBlank()) {
            return UNKNOWN_KEY;
        }
        return guid;
    }

    private void cleanupStale() {
        long now = Instant.now().getEpochSecond();
        for (Map.Entry<String, FragmentState> e : states.entrySet()) {
            FragmentState s = e.getValue();
            if (now - s.lastUpdate > timeoutSeconds) {
                states.remove(e.getKey());
            }
        }
    }

    private static class FragmentState {
        volatile int expectedTotal;
        final Map<Integer, String> parts = new TreeMap<>();
        volatile long lastUpdate = Instant.now().getEpochSecond();

        FragmentState(int expectedTotal) {
            this.expectedTotal = expectedTotal;
        }
    }
}
