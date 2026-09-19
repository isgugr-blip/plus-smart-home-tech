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
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;
import ru.yandex.practicum.order.feign.inventory.InventoryClient;
import ru.yandex.practicum.order.feign.inventory.ReserveRequest;
import ru.yandex.practicum.order.feign.inventory.ReserveResponse;
import ru.yandex.practicum.order.feign.product.ProductClient;
import ru.yandex.practicum.order.feign.product.ProductDto;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Map<Long, ServiceCallResult<ProductDto>> products = new HashMap<>();
        request.items().forEach(item -> products.computeIfAbsent(item.productId(), this::fetchProduct));

        Map<Long, Integer> quantities = new LinkedHashMap<>();
        request.items().forEach(item -> quantities.merge(item.productId(), item.quantity(), Integer::sum));

        Set<String> degradations = new LinkedHashSet<>();
        products.values().stream()
                .filter(ServiceCallResult::isDegraded)
                .forEach(result -> degradations.add(result.degradationReason()));

        Map<Long, Integer> reserved = new LinkedHashMap<>();
        try {
            quantities.forEach((productId, quantity) -> {
                ServiceCallResult<ReserveResponse> result = reserve(productId, quantity);
                if (result.isDegraded()) {
                    degradations.add(result.degradationReason());
                } else {
                    reserved.put(productId, quantity);
                }
            });
            return toDto(repository.save(buildOrder(request, products, degradations)));
        } catch (RuntimeException e) {
            releaseAll(reserved);
            throw e;
        }
    }

    private Order buildOrder(CreateOrderRequest request,
                             Map<Long, ServiceCallResult<ProductDto>> products,
                             Set<String> degradations) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus(degradations.isEmpty() ? OrderStatus.CONFIRMED : OrderStatus.PENDING_CONFIRMATION);
        if (!degradations.isEmpty()) {
            order.setStatusDetails("Заказ требует ручной проверки: " + String.join("; ", degradations));
        }
        request.items().forEach(item -> {
            ProductDto product = products.get(item.productId()).value();
            String name = product != null
                    ? product.name()
                    : "Товар #" + item.productId() + " (ожидает проверки)";
            BigDecimal price = product != null ? product.price() : BigDecimal.ZERO;
            order.addItem(new OrderItem(item.productId(), name, item.quantity(), price));
        });
        order.setTotalPrice(order.getItems().stream()
                .map(OrderItem::getSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        return order;
    }

    private ServiceCallResult<ProductDto> fetchProduct(Long productId) {
        ProductDto product;
        try {
            product = productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Товар id=" + productId + " не найден в каталоге");
        } catch (ProductServiceUnavailableException e) {
            return ServiceCallResult.degraded(
                    "каталог недоступен, данные товара id=" + productId + " не получены");
        } catch (FeignException e) {
            log.error("Ошибка вызова product-service для товара id={}: {}", productId, e.status(), e);
            throw new OrderProcessingException("Не удалось получить данные товара id=" + productId);
        }
        if (!Boolean.TRUE.equals(product.active())) {
            throw new OrderProcessingException("Товар id=" + productId + " снят с продажи");
        }
        return ServiceCallResult.success(product);
    }

    private ServiceCallResult<ReserveResponse> reserve(Long productId, Integer quantity) {
        ReserveResponse response;
        try {
            response = inventoryClient.reserve(new ReserveRequest(productId, quantity));
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Складская запись для товара id=" + productId + " не найдена");
        } catch (FeignException.Conflict e) {
            throw new OrderProcessingException(
                    "Недостаточно товара id=" + productId + " на складе: запрошено " + quantity);
        } catch (InventoryServiceUnavailableException e) {
            return ServiceCallResult.degraded(
                    "склад недоступен, резерв " + quantity + " шт. товара id=" + productId + " не подтверждён");
        } catch (FeignException e) {
            log.error("Ошибка вызова inventory-service для товара id={}: {}", productId, e.status(), e);
            throw new OrderProcessingException("Не удалось зарезервировать товар id=" + productId);
        }
        if (!response.success()) {
            throw new OrderProcessingException(
                    "Недостаточно товара id=" + productId + " на складе: запрошено " + quantity);
        }
        return ServiceCallResult.success(response);
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
