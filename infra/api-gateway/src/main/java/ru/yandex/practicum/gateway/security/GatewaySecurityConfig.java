package ru.yandex.practicum.gateway.security;

import org.springframework.cloud.gateway.config.GlobalCorsProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.List;

@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {

    private static final String[] PUBLIC_DOCS = {
            "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html", "/webjars/**"
    };

    private static final String[] CATALOG_PATHS = {
            "/api/products/**", "/api/categories/**", "/api/inventory/**"
    };

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .logout(ServerHttpSecurity.LogoutSpec::disable)
                .cors(Customizer.withDefaults())
                .httpBasic(Customizer.withDefaults())
                .authorizeExchange(exchange -> exchange
                        .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .pathMatchers(PUBLIC_DOCS).permitAll()
                        .pathMatchers(HttpMethod.GET, CATALOG_PATHS).permitAll()
                        .pathMatchers(HttpMethod.GET, "/api/orders").hasRole("ADMIN")
                        .pathMatchers("/api/orders/**").hasRole("USER")
                        .pathMatchers(CATALOG_PATHS).hasRole("ADMIN")
                        .anyExchange().denyAll()
                )
                .build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource(GlobalCorsProperties corsProperties) {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        corsProperties.getCorsConfigurations().forEach(source::registerCorsConfiguration);
        return source;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    public MapReactiveUserDetailsService userDetailsService(GatewayUsersProperties properties,
                                                            PasswordEncoder passwordEncoder) {
        List<UserDetails> users = properties.users().stream()
                .map(user -> (UserDetails) User.withUsername(user.username())
                        .password(passwordEncoder.encode(user.password()))
                        .roles(user.roles().toArray(String[]::new))
                        .build())
                .toList();
        return new MapReactiveUserDetailsService(users);
    }
}
