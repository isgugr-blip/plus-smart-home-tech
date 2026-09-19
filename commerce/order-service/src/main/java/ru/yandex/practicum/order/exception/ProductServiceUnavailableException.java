package ru.yandex.practicum.order.exception;

public class ProductServiceUnavailableException extends RuntimeException {

    public ProductServiceUnavailableException(Long productId, Throwable cause) {
        super("product-service недоступен: не удалось получить данные товара id=" + productId, cause);
    }
}
