package com.rce.reverseadapterdemoapp;

import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class Controller {
    final List<WebClient> targets;
    final List<WebClient> unversionedTargets;
    final String target_version;

    public Controller() {
        String[] apps = Optional.ofNullable(System.getenv().get("TARGET_APPS")).map(a -> a.split(", ")).orElse(new String[0]);
        target_version = System.getenv().get("TARGET_VERSION");

        targets = new ArrayList<>();
        unversionedTargets = new ArrayList<>();

        for (String app : apps) {
            String host = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase() + "_SERVICE_HOST");
            String port = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase()  + "_SERVICE_PORT");

            if (host == null || port == null)
                continue;

            targets.add(WebClient.create("http://" + host + ":" + port));
        }

        for (String app : apps) {
            String host = System.getenv().get(app.toUpperCase() + "_SERVICE_HOST");
            String port = System.getenv().get(app.toUpperCase() + "_SERVICE_PORT");

            if (host == null || port == null)
                continue;

            unversionedTargets.add(WebClient.create("http://" + host + ":" + port));
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

            ThreadLocalRandom.current().ints(0, targets.size()).distinct().limit(Math.min(fanout, targets.size())).forEach(i -> {
                WebClient target = targets.get(i);
                l.add(send(target, body, maxCalls, callsMade, fanout));
            });

            return Mono.zip(l, a -> (ResponseEntity<Schema>) a[0]).block();
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(body);
        }
    }

    @RequestMapping(
            value = "/demo",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ResponseEntity<UnversionedSchema> procedure2(
            @RequestHeader("maxcalls") int maxCalls, @RequestHeader("calls") int calls, @RequestHeader("fanout") int fanout,
            @RequestBody UnversionedSchema body
    ) {
        try {

            if(calls >= maxCalls || targets.isEmpty()) {
                return ResponseEntity.ok(body);
            }

            final int callsMade = calls+1;

            List<Mono<ResponseEntity<UnversionedSchema>>> l = new ArrayList<>(Math.min(fanout, unversionedTargets.size()));

            ThreadLocalRandom.current().ints(0, unversionedTargets.size()).distinct().limit(Math.min(fanout, unversionedTargets.size())).forEach(i -> {
                WebClient target = unversionedTargets.get(i);
                l.add(sendUnversioned(target, body, maxCalls, callsMade, fanout));
            });

            return Mono.zip(l, a -> (ResponseEntity<UnversionedSchema>) a[0]).block();
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(body);
        }
    }

    public Mono<ResponseEntity<Schema>> send(WebClient client, Schema body, int maxCalls, int calls, int fanout) {
        WebClient.RequestBodySpec requestBodySpec = client
                .method(HttpMethod.POST)
                .uri("/" + target_version.toLowerCase() + "/demo");

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

    public Mono<ResponseEntity<UnversionedSchema>> sendUnversioned(WebClient client, UnversionedSchema body, int maxCalls, int calls, int fanout) {
        WebClient.RequestBodySpec requestBodySpec = client
                .method(HttpMethod.POST)
                .uri("/demo");

        requestBodySpec.header("maxcalls", String.valueOf(maxCalls));
        requestBodySpec.header("calls", String.valueOf(calls));
        requestBodySpec.header("fanout", String.valueOf(fanout));

        return requestBodySpec
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(body))
                .retrieve()
                .toEntity(UnversionedSchema.class);
    }
}
