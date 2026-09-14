package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.AlreadyExistsException;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {

    private final InventoryRepository repository;

    public List<InventoryDto> findAll() {
        return repository.findAll().stream().map(InventoryService::toDto).toList();
    }

    public InventoryDto findByProductId(Long productId) {
        return toDto(getItem(productId));
    }

    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        if (repository.findByProductId(request.productId()).isPresent()) {
            throw new AlreadyExistsException(
                    "Складская запись для товара id=" + request.productId() + " уже существует");
        }
        return toDto(repository.save(new InventoryItem(request.productId(), request.quantity())));
    }

    @Transactional
    public InventoryDto updateQuantity(UpdateInventoryRequest request) {
        InventoryItem item = getItem(request.productId());
        if (request.quantity() < item.getReservedQuantity()) {
            throw new InsufficientStockException("Нельзя установить количество %d: уже зарезервировано %d"
                    .formatted(request.quantity(), item.getReservedQuantity()));
        }
        item.setQuantity(request.quantity());
        return toDto(repository.save(item));
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        InventoryItem item = getItem(request.productId());
        if (item.getAvailableQuantity() < request.quantity()) {
            throw new InsufficientStockException(
                    "Недостаточно товара id=%d: запрошено %d, доступно %d"
                            .formatted(request.productId(), request.quantity(), item.getAvailableQuantity()));
        }
        item.setReservedQuantity(item.getReservedQuantity() + request.quantity());
        InventoryItem saved = repository.save(item);
        return new ReserveResponse(true, saved.getAvailableQuantity(),
                "Зарезервировано %d шт. товара id=%d".formatted(request.quantity(), request.productId()));
    }

    private InventoryItem getItem(Long productId) {
        return repository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Складская запись для товара id=" + productId + " не найдена"));
    }

    private static InventoryDto toDto(InventoryItem item) {
        return new InventoryDto(item.getId(), item.getProductId(), item.getQuantity(),
                item.getReservedQuantity(), item.getAvailableQuantity());
    }
}
