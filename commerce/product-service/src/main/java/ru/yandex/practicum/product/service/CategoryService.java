package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateCategoryRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.repository.CategoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CategoryService {

    private final CategoryRepository repository;

    public List<CategoryDto> findAll() {
        return repository.findAll().stream().map(CatalogMapper::toDto).toList();
    }

    public CategoryDto findById(Long id) {
        return CatalogMapper.toDto(getCategory(id));
    }

    @Transactional
    public CategoryDto create(CreateCategoryRequest request) {
        Category saved = repository.save(new Category(request.name(), request.description()));
        return CatalogMapper.toDto(saved);
    }

    Category getCategory(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Категория с id=" + id + " не найдена"));
    }
}
