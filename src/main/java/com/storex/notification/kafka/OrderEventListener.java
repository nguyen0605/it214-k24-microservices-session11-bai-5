package com.storex.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.storex.notification.model.OrderEvent;
import com.storex.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final NotificationService notificationService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private final Map<String, Boolean> processedOrders = new ConcurrentHashMap<>();

    private static final String DLQ_TOPIC = "storex-order-events.DLQ";

    @KafkaListener(topics = "storex-order-events", groupId = "notification-group")
    public void listen(ConsumerRecord<String, String> record) {
        String payload = record.value();
        OrderEvent event;

        try {
            event = objectMapper.readValue(payload, OrderEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialize message: {}", payload, e);
            return;
        }

        String orderId = event.getOrderId();

        // BUG-06: Idempotency check
        if (processedOrders.putIfAbsent(orderId, Boolean.TRUE) != null) {
            log.info("BUG-06: Duplicate message detected for orderId = {}. Skipping processing.", orderId);
            return;
        }

        // REQ-01: Non-blocking execution using subscribe
        notificationService.processNotification(event)
                .subscribe(
                        success -> log.info("Successfully processed notification for orderId = {}", orderId),
                        error -> {
                            // BUG-07: Push to DLQ if max retries exceeded
                            log.error("BUG-07: Đã đẩy order {} vào DLQ do lỗi gửi thông báo. Chi tiết: {}", orderId, error.getMessage());
                            kafkaTemplate.send(DLQ_TOPIC, orderId, payload);
                        }
                );
    }
}