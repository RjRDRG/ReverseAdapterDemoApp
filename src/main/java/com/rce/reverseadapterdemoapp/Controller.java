package com.rce.reverseadapterdemoapp;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class Controller {
    final List<URI> targets;

    public Controller() {
        String[] apps = Optional.ofNullable(System.getenv().get("TARGET_APPS")).map(a -> a.split(", ")).orElse(new String[0]);
        String target_version = System.getenv().get("TARGET_VERSION");

        targets = new ArrayList<>();

        for (String app : apps) {
            String host = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase() + "_SERVICE_HOST");
            String port = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase()  + "_SERVICE_PORT");

            if (host == null || port == null)
                continue;

            URI uri = UriComponentsBuilder
                    .newInstance()
                    .scheme("http")
                    .host(host)
                    .port(port)
                    .path("/" + target_version.toLowerCase() + "/demo")
                    .build().toUri();

            targets.add(uri);
        }
    }

    public static final String TEMPLATE_DEMO_POST = "Controller.procedure(Integer.parseInt(#header|maxCalls#), Integer.parseInt(#header|calls#), Integer.parseInt(#header|fanout#), objectMapper.readValue(#json#, Schema.class))";

    @RequestMapping(
            value = "/v0/demo",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<Schema> procedure(
            @RequestHeader("maxcalls") int maxCalls, @RequestHeader("calls") int calls, @RequestHeader("fanout") int fanout,
            @RequestBody Schema body
    ) {
        try {

            if(calls >= maxCalls || targets.isEmpty()) {
                return ResponseEntity.ok(body);
            }

            final int callsMade = calls+1;

            List<Mono<ResponseEntity<Schema>>> l = new ArrayList<>(Math.min(fanout, targets.size()));

            ThreadLocalRandom.current().ints(0, targets.size()).distinct().limit(fanout).forEach(i -> {
                URI target = targets.get(i);
                l.add(send(body, target, maxCalls, callsMade, fanout));
            });

            return Mono.zip(l, a -> (ResponseEntity<Schema>) a[0]).block();
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(body);
        }
    }

    public Mono<ResponseEntity<Schema>> send(Schema body, URI target, int maxCalls, int calls, int fanout) {
        WebClient.RequestBodySpec requestBodySpec = WebClient.create()
                .method(HttpMethod.POST)
                .uri(target);

        requestBodySpec.header("maxcalls", String.valueOf(maxCalls));
        requestBodySpec.header("calls", String.valueOf(calls));
        requestBodySpec.header("fanout", String.valueOf(fanout));

        return requestBodySpec
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(body))
                .retrieve()
                .toEntity(Schema.class);
    }
}
