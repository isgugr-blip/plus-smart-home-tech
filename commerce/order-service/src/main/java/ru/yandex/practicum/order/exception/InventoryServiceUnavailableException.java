package ru.yandex.practicum.order.exception;

public class InventoryServiceUnavailableException extends RuntimeException {

    public InventoryServiceUnavailableException(Long productId, Throwable cause) {
        super("inventory-service недоступен: не удалось выполнить складскую операцию для товара id=" + productId,
                cause);
    }
}
