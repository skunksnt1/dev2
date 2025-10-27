package com.btg.pactual.hub.news.constants;
public final class AppConstants {
    public static final boolean SAVE_TO_FILE =
        Boolean.parseBoolean(System.getenv().getOrDefault("SAVE_TO_FILE", "false"));
    public static final String SAVE_PATH = "data/mrn/";
    private AppConstants() {}
}
