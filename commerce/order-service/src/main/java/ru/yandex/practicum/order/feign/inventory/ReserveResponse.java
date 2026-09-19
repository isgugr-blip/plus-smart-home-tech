package ru.yandex.practicum.order.feign.inventory;

public record ReserveResponse(
        boolean success,
        Integer availableQuantity,
        String message
) {
}
