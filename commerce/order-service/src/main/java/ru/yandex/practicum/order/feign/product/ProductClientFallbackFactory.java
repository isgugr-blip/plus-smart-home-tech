package ru.yandex.practicum.order.feign.product;

import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;
import ru.yandex.practicum.order.feign.FeignErrors;

@Component
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {

    private static final Logger log = LoggerFactory.getLogger(ProductClientFallbackFactory.class);

    @Override
    public ProductClient create(Throwable cause) {
        return productId -> {
            FeignException.FeignClientException businessError = FeignErrors.businessError(cause);
            if (businessError != null) {
                throw businessError;
            }
            log.warn("product-service: получение товара id={} не выполнено, причина деградации: {}",
                    productId, cause.toString());
            throw new ProductServiceUnavailableException(productId, cause);
        };
    }
}
