package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository repository;
    private final CategoryService categoryService;

    public List<ProductDto> findAllActive() {
        return toDtos(repository.findByActiveTrue());
    }

    public ProductDto findById(Long id) {
        return CatalogMapper.toDto(getProduct(id));
    }

    public List<ProductDto> findByCategory(Long categoryId) {
        categoryService.getCategory(categoryId);
        return toDtos(repository.findByCategoryIdAndActiveTrue(categoryId));
    }

    public List<ProductDto> search(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Параметр query обязателен и не может быть пустым");
        }
        return toDtos(repository.findByActiveTrueAndNameContainingIgnoreCase(query.trim()));
    }

    @Transactional
    public ProductDto create(CreateProductRequest request) {
        Product product = new Product();
        product.setName(request.name());
        product.setDescription(request.description());
        product.setPrice(request.price());
        product.setImageUrl(request.imageUrl());
        product.setActive(true);
        if (request.categoryId() != null) {
            product.setCategory(categoryService.getCategory(request.categoryId()));
        }
        return CatalogMapper.toDto(repository.save(product));
    }

    @Transactional
    public ProductDto update(Long id, UpdateProductRequest request) {
        Product product = getProduct(id);
        if (request.name() != null) {
            product.setName(request.name());
        }
        if (request.description() != null) {
            product.setDescription(request.description());
        }
        if (request.price() != null) {
            product.setPrice(request.price());
        }
        if (request.imageUrl() != null) {
            product.setImageUrl(request.imageUrl());
        }
        if (request.active() != null) {
            product.setActive(request.active());
        }
        if (request.categoryId() != null) {
            product.setCategory(categoryService.getCategory(request.categoryId()));
        }
        return CatalogMapper.toDto(repository.save(product));
    }

    private Product getProduct(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Товар с id=" + id + " не найден"));
    }

    private static List<ProductDto> toDtos(List<Product> products) {
        return products.stream().map(CatalogMapper::toDto).toList();
    }
}
