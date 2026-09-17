package com.storex.notification.service;

import com.storex.notification.model.OrderEvent;
import com.storex.notification.model.PreferenceResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Slf4j
@Service
public class NotificationService {

    private final WebClient preferenceWebClient;
    private final WebClient notifyWebClient;

    public NotificationService(
            @Value("${app.external.preference-api}") String preferenceApiUrl,
            @Value("${app.external.notify-api}") String notifyApiUrl) {
        this.preferenceWebClient = WebClient.create(preferenceApiUrl);
        this.notifyWebClient = WebClient.create(notifyApiUrl);
    }

    public Mono<Void> processNotification(OrderEvent event) {
        return getPreference(event.getUserId())
                .flatMap(channel -> sendNotification(channel, event));
    }

    private Mono<String> getPreference(String userId) {
        return preferenceWebClient.get()
                .uri("/api/preferences/{userId}", userId)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> Mono.error(new RuntimeException("Preference API error: " + response.statusCode())))
                .bodyToMono(PreferenceResponse.class)
                .map(PreferenceResponse::getChannel)
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1))
                        .filter(throwable -> true)
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            log.warn("Preference API failed after retries, applying Fallback: EMAIL");
                            return retrySignal.failure();
                        }))
                .onErrorResume(ex -> {
                    log.warn("BUG-05: Error fetching preference for user {}. Fallback to EMAIL. Reason: {}", userId, ex.getMessage());
                    return Mono.just("EMAIL");
                });
    }

    private Mono<Void> sendNotification(String channel, OrderEvent event) {
        String endpoint = "EMAIL".equalsIgnoreCase(channel) ? "/api/notify/email" : "/api/notify/zalo";

        return notifyWebClient.post()
                .uri(endpoint)
                .bodyValue(event)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> Mono.error(new RuntimeException("Notify API error: " + response.statusCode())))
                .bodyToMono(Void.class)
                .timeout(Duration.ofSeconds(3))
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            throw new RuntimeException("Notify API failed after 2 retries");
                        }));
    }
}