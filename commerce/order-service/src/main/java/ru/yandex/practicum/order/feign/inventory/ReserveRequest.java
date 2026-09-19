package ru.yandex.practicum.order.feign.inventory;

public record ReserveRequest(
        Long productId,
        Integer quantity
) {
}
