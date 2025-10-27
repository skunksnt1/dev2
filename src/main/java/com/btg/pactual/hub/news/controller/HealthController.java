package com.btg.pactual.hub.news.controller;
import org.springframework.web.bind.annotation.*; @RestController
public class HealthController { @GetMapping("/health") public String ok(){ return "ok"; } }
