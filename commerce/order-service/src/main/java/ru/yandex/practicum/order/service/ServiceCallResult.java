package ru.yandex.practicum.order.service;

public record ServiceCallResult<T>(T value, String degradationReason) {

    public static <T> ServiceCallResult<T> success(T value) {
        return new ServiceCallResult<>(value, null);
    }

    public static <T> ServiceCallResult<T> degraded(String degradationReason) {
        return new ServiceCallResult<>(null, degradationReason);
    }

    public boolean isDegraded() {
        return degradationReason != null;
    }
}
