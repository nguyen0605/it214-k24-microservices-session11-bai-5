package com.storex.notification.model;

import lombok.Data;

@Data
public class OrderEvent {
    private String orderId;
    private String userId;
    private String item;
    private double amount;
}