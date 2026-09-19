package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.feign.inventory.InventoryClient;
import ru.yandex.practicum.order.feign.inventory.ReserveRequest;
import ru.yandex.practicum.order.feign.inventory.ReserveResponse;
import ru.yandex.practicum.order.feign.product.ProductClient;
import ru.yandex.practicum.order.feign.product.ProductDto;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final ProductClient productClient;
    private final InventoryClient inventoryClient;
    private final OrderRepository repository;

    public List<OrderDto> findAll() {
        return repository.findAll().stream().map(OrderService::toDto).toList();
    }

    public OrderDto findById(Long id) {
        return toDto(repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден")));
    }

    public List<OrderDto> findByEmail(String email) {
        return repository.findByCustomerEmailIgnoreCaseOrderByIdDesc(email).stream().map(OrderService::toDto).toList();
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public OrderDto create(CreateOrderRequest request) {
        Map<Long, ProductDto> products = new HashMap<>();
        request.items().forEach(item -> products.computeIfAbsent(item.productId(), this::fetchProduct));

        Map<Long, Integer> quantities = new LinkedHashMap<>();
        request.items().forEach(item -> quantities.merge(item.productId(), item.quantity(), Integer::sum));

        Map<Long, Integer> reserved = new LinkedHashMap<>();
        try {
            quantities.forEach((productId, quantity) -> {
                reserve(productId, quantity);
                reserved.put(productId, quantity);
            });

            Order order = new Order();
            order.setCustomerName(request.customerName());
            order.setCustomerEmail(request.customerEmail());
            order.setStatus(OrderStatus.CONFIRMED);
            request.items().forEach(item -> {
                ProductDto product = products.get(item.productId());
                order.addItem(new OrderItem(item.productId(), product.name(), item.quantity(), product.price()));
            });
            order.setTotalPrice(order.getItems().stream()
                    .map(OrderItem::getSubtotal)
                    .reduce(BigDecimal.ZERO, BigDecimal::add));
            return toDto(repository.save(order));
        } catch (RuntimeException e) {
            releaseAll(reserved);
            throw e;
        }
    }

    private ProductDto fetchProduct(Long productId) {
        ProductDto product;
        try {
            product = productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Товар id=" + productId + " не найден в каталоге");
        } catch (FeignException e) {
            log.error("Ошибка вызова product-service для товара id={}: {}", productId, e.status(), e);
            throw new OrderProcessingException("Не удалось получить данные товара id=" + productId);
        }
        if (!Boolean.TRUE.equals(product.active())) {
            throw new OrderProcessingException("Товар id=" + productId + " снят с продажи");
        }
        return product;
    }

    private void reserve(Long productId, Integer quantity) {
        ReserveResponse response;
        try {
            response = inventoryClient.reserve(new ReserveRequest(productId, quantity));
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Складская запись для товара id=" + productId + " не найдена");
        } catch (FeignException.Conflict e) {
            throw new OrderProcessingException(
                    "Недостаточно товара id=" + productId + " на складе: запрошено " + quantity);
        } catch (FeignException e) {
            log.error("Ошибка вызова inventory-service для товара id={}: {}", productId, e.status(), e);
            throw new OrderProcessingException("Не удалось зарезервировать товар id=" + productId);
        }
        if (!response.success()) {
            throw new OrderProcessingException(
                    "Недостаточно товара id=" + productId + " на складе: запрошено " + quantity);
        }
    }

    private void releaseAll(Map<Long, Integer> reserved) {
        reserved.forEach((productId, quantity) -> {
            try {
                inventoryClient.release(new ReserveRequest(productId, quantity));
            } catch (RuntimeException e) {
                log.error("Не удалось снять резерв {} шт. товара id={}", quantity, productId, e);
            }
        });
    }

    private static OrderDto toDto(Order order) {
        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus().name(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                order.getItems().stream()
                        .map(i -> new OrderItemDto(i.getId(), i.getProductId(), i.getProductName(),
                                i.getQuantity(), i.getPrice()))
                        .toList()
        );
    }
}
