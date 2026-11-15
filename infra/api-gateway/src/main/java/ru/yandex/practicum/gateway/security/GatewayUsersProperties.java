package ru.yandex.practicum.gateway.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "gateway.security")
public record GatewayUsersProperties(List<GatewayUser> users) {

    public record GatewayUser(String username, String password, List<String> roles) {
    }
}
