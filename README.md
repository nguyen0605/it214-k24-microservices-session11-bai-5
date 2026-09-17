# Notification Service - Tích hợp WebClient & Kafka

## 1. Tổng quan kiến trúc
Hệ thống thực hiện tiêu thụ sự kiện từ Kafka (`storex-order-events`), giao tiếp không chặn (non-blocking) với các dịch vụ ngoại vi bằng WebClient, xử lý cơ chế Retry, Timeout, Fallback và đảm bảo tính Idempotency.

```mermaid
graph TD
    A[Kafka: storex-order-events] -->|Consumer| B(OrderEventListener)
    B -->|Check Idempotency| C{Processed?}
    C -->|Yes| D[Skip & Commit Offset]
    C -->|No| E[NotificationService]
    E -->|GET /api/preferences/{userId}| F[Preference API]
    F -->|Timeout/Error 5xx| G[Fallback: EMAIL]
    E -->|POST /api/notify/...| H[Email/Zalo API]
    H -->|Success| I[Done]
    H -->|Failed after Retries| J[DLQ: storex-order-events.DLQ]
```

## 2. Giải quyết Edge Cases
- **BUG-05**: WebClient bắt lỗi 5xx hoặc Connection Refused khi gọi User Preference API, ghi log WARNING và thực hiện Fallback chuyển sang kênh EMAIL.
- **BUG-06**: Sử dụng `ConcurrentHashMap.putIfAbsent()` để lưu trữ `orderId` nhằm loại bỏ message trùng lặp (Idempotency).
- **BUG-07**: Khi API gửi thông báo thất bại hoàn toàn sau 2 lần retry, hệ thống bắt lỗi trong luồng Reactive, chuyển message vào Dead Letter Queue (`storex-order-events.DLQ`) và ghi log ở mức ERROR đúng thông điệp yêu cầu.