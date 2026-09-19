package ru.yandex.practicum.order.feign.inventory;

import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.feign.FeignErrors;

@Component
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    private static final Logger log = LoggerFactory.getLogger(InventoryClientFallbackFactory.class);

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {

            @Override
            public ReserveResponse reserve(ReserveRequest request) {
                return degrade("резервирование", request);
            }

            @Override
            public ReserveResponse release(ReserveRequest request) {
                return degrade("снятие резерва", request);
            }

            private ReserveResponse degrade(String operation, ReserveRequest request) {
                FeignException.FeignClientException businessError = FeignErrors.businessError(cause);
                if (businessError != null) {
                    throw businessError;
                }
                log.warn("inventory-service: {} {} шт. товара id={} не выполнено, причина деградации: {}",
                        operation, request.quantity(), request.productId(), cause.toString());
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }
        };
    }
}
