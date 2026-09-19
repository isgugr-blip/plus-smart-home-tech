package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {

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

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus(OrderStatus.CREATED);
        request.items().forEach(item -> order.addItem(
                new OrderItem(item.productId(), item.productName(), item.quantity(), item.price())));
        order.setTotalPrice(order.getItems().stream()
                .map(OrderItem::getSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
        return toDto(repository.save(order));
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
