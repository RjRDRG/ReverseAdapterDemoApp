package com.rce.reverseadapterdemoapp;

import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class Controller {
    final List<WebClient> targets;
    final String target_version;

    public Controller() {
        String[] apps = Optional.ofNullable(System.getenv().get("TARGET_APPS")).map(a -> a.split(", ")).orElse(new String[0]);
        target_version = System.getenv().get("TARGET_VERSION");

        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(30))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(EpollChannelOption.TCP_KEEPIDLE, 300)
                .option(EpollChannelOption.TCP_KEEPINTVL, 60)
                .option(EpollChannelOption.TCP_KEEPCNT, 8);

        WebClient.Builder builder = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient));

        targets = new ArrayList<>();

        for (String app : apps) {
            String host = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase() + "_SERVICE_HOST");
            String port = System.getenv().get(app.toUpperCase() + "_" + target_version.toUpperCase()  + "_SERVICE_PORT");

            if (host == null || port == null)
                continue;

            targets.add(builder.baseUrl("http://" + host + ":" + port).build());
        }
    }

    public static final String TEMPLATE_DEMO_POST = "Controller.procedure(Integer.parseInt(#header|maxCalls#), Integer.parseInt(#header|calls#), Integer.parseInt(#header|fanout#), objectMapper.readValue(#json#, Schema.class))";

    @RequestMapping(
            value = "/v1/demo",
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
                WebClient target = targets.get(i);
                l.add(send(target, body, maxCalls, callsMade, fanout));
            });

            return Mono.zip(l, a -> (ResponseEntity<Schema>) a[0]).block();
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
}
