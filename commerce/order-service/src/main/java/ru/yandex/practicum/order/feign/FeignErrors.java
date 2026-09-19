package ru.yandex.practicum.order.feign;

import feign.FeignException;

public final class FeignErrors {

    private FeignErrors() {
    }

    public static FeignException.FeignClientException businessError(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof FeignException.FeignClientException clientException) {
                return clientException;
            }
        }
        return null;
    }
}
