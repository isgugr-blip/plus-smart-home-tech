package ru.yandex.practicum.order.feign;

import feign.RequestInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Optional;
import java.util.UUID;

@Configuration
public class FeignConfig {

    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    @Bean
    public RequestInterceptor requestIdInterceptor() {
        return template -> {
            template.header("X-Source-Service", "order-service");
            template.header(REQUEST_ID_HEADER, incomingRequestId().orElseGet(() -> UUID.randomUUID().toString()));
        };
    }

    private static Optional<String> incomingRequestId() {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            return Optional.ofNullable(attributes.getRequest().getHeader(REQUEST_ID_HEADER))
                    .filter(id -> !id.isBlank());
        }
        return Optional.empty();
    }
}
