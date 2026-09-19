package ru.yandex.practicum.order;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import feign.Request;
import feign.RequestTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;
import ru.yandex.practicum.order.feign.inventory.InventoryClient;
import ru.yandex.practicum.order.feign.inventory.ReserveRequest;
import ru.yandex.practicum.order.feign.inventory.ReserveResponse;
import ru.yandex.practicum.order.feign.product.ProductClient;
import ru.yandex.practicum.order.feign.product.ProductDto;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest
@AutoConfigureMockMvc
@SuppressWarnings("unchecked")
class OrderServiceAcceptanceTest {

    private static final ProductDto LAMP =
            new ProductDto(1L, "Acceptance Smart Lamp", "лампа", new BigDecimal("3490.00"), true);
    private static final ProductDto PLUG =
            new ProductDto(2L, "Acceptance Smart Plug", "розетка", new BigDecimal("1290.00"), true);

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper json;

    @Autowired
    private OrderRepository repository;

    @MockBean
    private ProductClient productClient;

    @MockBean
    private InventoryClient inventoryClient;

    @BeforeEach
    void setUp() {
        when(productClient.getProductById(1L)).thenReturn(LAMP);
        when(productClient.getProductById(2L)).thenReturn(PLUG);
        when(inventoryClient.reserve(any())).thenReturn(new ReserveResponse(true, 10, "ok"));
    }

    @Test
    void shouldFetchProductDataReserveStockAndStoreSnapshot() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(
                "Acceptance Buyer",
                "acceptance-buyer@example.com",
                List.of(new OrderItemRequest(1L, 2), new OrderItemRequest(2L, 1)));

        MvcResult createResponse = postJson("/api/orders", request);

        assertThat(status(createResponse))
                .as("POST /api/orders должен создавать заказ и возвращать HTTP 201 Created")
                .isEqualTo(201);
        Map<String, Object> created = readMap(createResponse);
        assertThat(created.get("status"))
                .as("Успешно зарезервированный заказ должен сохраняться в статусе CONFIRMED")
                .isEqualTo("CONFIRMED");
        assertThat(asDecimal(created.get("totalPrice")))
                .as("totalPrice считается по ценам, полученным из product-service")
                .isEqualByComparingTo("8270.00");
        assertThat((List<?>) created.get("items"))
                .as("Заказ должен хранить снимок товарных данных")
                .hasSize(2)
                .anySatisfy(item -> assertThat((Map<String, Object>) item)
                        .containsEntry("productName", "Acceptance Smart Lamp")
                        .containsEntry("quantity", 2));
        verify(inventoryClient).reserve(new ReserveRequest(1L, 2));
        verify(inventoryClient).reserve(new ReserveRequest(2L, 1));

        Long orderId = asLong(created.get("id"));
        MvcResult byIdResponse = mvc.perform(get("/api/orders/{id}", orderId)).andReturn();
        assertThat(status(byIdResponse)).isEqualTo(200);
        assertThat(readMap(byIdResponse).get("customerEmail")).isEqualTo("acceptance-buyer@example.com");

        MvcResult byEmailResponse = mvc.perform(get("/api/orders/by-email")
                        .param("email", "acceptance-buyer@example.com"))
                .andReturn();
        assertThat(status(byEmailResponse)).isEqualTo(200);
        assertThat(readList(byEmailResponse))
                .anySatisfy(item -> assertThat(item).containsEntry("customerEmail", "acceptance-buyer@example.com"));
    }

    @Test
    void shouldRequestProductOnceAndReserveSummedQuantityForRepeatedProduct() throws Exception {
        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 2), new OrderItemRequest(1L, 1)));

        assertThat(status(response)).isEqualTo(201);
        verify(productClient, times(1))
                .getProductById(1L);
        verify(inventoryClient).reserve(new ReserveRequest(1L, 3));
        assertThat(asDecimal(readMap(response).get("totalPrice")))
                .as("Обе позиции должны попасть в заказ и в итоговую стоимость")
                .isEqualByComparingTo("10470.00");
    }

    @Test
    void shouldRejectOrderWhenProductNotFound() throws Exception {
        when(productClient.getProductById(99L)).thenThrow(feignException(404));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(99L, 1)));

        assertThat(status(response))
                .as("Несуществующий товар — HTTP 422 Unprocessable Entity")
                .isEqualTo(422);
        assertThat(readMap(response).get("message").toString())
                .as("Клиенту не показываем сырой текст FeignException")
                .doesNotContain("FeignException")
                .contains("не найден");
        verify(inventoryClient, never()).reserve(any());
    }

    @Test
    void shouldRejectOrderWhenProductIsInactive() throws Exception {
        when(productClient.getProductById(3L))
                .thenReturn(new ProductDto(3L, "Снятый с продажи", null, new BigDecimal("100.00"), false));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(3L, 1)));

        assertThat(status(response)).isEqualTo(422);
        assertThat(readMap(response).get("message").toString()).contains("снят с продажи");
        verify(inventoryClient, never()).reserve(any());
    }

    @Test
    void shouldRejectOrderWhenInventoryRecordNotFound() throws Exception {
        when(inventoryClient.reserve(any())).thenThrow(feignException(404));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 1)));

        assertThat(status(response)).isEqualTo(422);
        assertThat(readMap(response).get("message").toString()).contains("Складская запись");
    }

    @Test
    void shouldRejectOrderWhenStockIsInsufficient() throws Exception {
        when(inventoryClient.reserve(any())).thenThrow(feignException(409));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 500)));

        assertThat(status(response))
                .as("409 от склада — бизнес-отказ, а не техническая деградация")
                .isEqualTo(422);
        assertThat(readMap(response).get("message").toString()).contains("Недостаточно товара");
    }

    @Test
    void shouldRejectOrderWhenNeighbourServiceFailsWithOtherError() throws Exception {
        when(inventoryClient.reserve(any())).thenThrow(feignException(500));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 1)));

        assertThat(status(response)).isEqualTo(422);
        assertThat(readMap(response).get("message").toString()).doesNotContain("FeignException");
    }

    @Test
    void shouldReleaseAlreadyCreatedReservesWhenScenarioFailsLater() throws Exception {
        when(inventoryClient.reserve(new ReserveRequest(1L, 2)))
                .thenReturn(new ReserveResponse(true, 5, "ok"));
        when(inventoryClient.reserve(new ReserveRequest(2L, 1)))
                .thenThrow(feignException(409));
        long ordersBefore = repository.count();

        MvcResult response = postJson("/api/orders",
                order(new OrderItemRequest(1L, 2), new OrderItemRequest(2L, 1)));

        assertThat(status(response)).isEqualTo(422);
        verify(inventoryClient)
                .release(new ReserveRequest(1L, 2));
        assertThat(repository.count())
                .as("Заказ не должен сохраняться при сорванном сценарии")
                .isEqualTo(ordersBefore);
    }

    @Test
    void shouldReturnBadRequestForInvalidOrderPayload() throws Exception {
        MvcResult response = postJson("/api/orders", new CreateOrderRequest("", "not-an-email", List.of()));

        assertThat(status(response))
                .as("POST /api/orders с невалидным телом запроса должен возвращать HTTP 400 Bad Request")
                .isEqualTo(400);
        assertThat(readMap(response)).containsKeys("message", "validationErrors");
    }

    private static CreateOrderRequest order(OrderItemRequest... items) {
        return new CreateOrderRequest("Acceptance Buyer", "acceptance-buyer@example.com", List.of(items));
    }

    private static FeignException feignException(int status) {
        Request request = Request.create(Request.HttpMethod.POST, "/stub", Map.of(), new byte[0],
                StandardCharsets.UTF_8, new RequestTemplate());
        return FeignException.errorStatus("stub",
                feign.Response.builder().status(status).request(request).build());
    }

    @Test
    void shouldSaveOrderAsPendingConfirmationWhenProductServiceIsUnavailable() throws Exception {
        when(productClient.getProductById(1L))
                .thenThrow(new ProductServiceUnavailableException(1L, new RuntimeException("connect timed out")));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 2)));

        assertThat(status(response))
                .as("Техническая недоступность каталога не должна отклонять заказ")
                .isEqualTo(201);
        Map<String, Object> created = readMap(response);
        assertThat(created.get("status"))
                .as("При недоступности product-service заказ сохраняется как PENDING_CONFIRMATION")
                .isEqualTo("PENDING_CONFIRMATION");
        assertThat(created.get("statusDetails").toString())
                .as("В деталях статуса должно быть указано, что заказ требует ручной проверки")
                .contains("ручной проверки");
        assertThat(asDecimal(created.get("totalPrice"))).isEqualByComparingTo("0.00");
        assertThat((List<Map<String, Object>>) created.get("items"))
                .as("Связь с выбранным товаром должна сохраниться даже без данных каталога")
                .singleElement()
                .satisfies(item -> {
                    assertThat(asLong(item.get("productId"))).isEqualTo(1L);
                    assertThat(item.get("productName")).isEqualTo("Товар #1 (ожидает проверки)");
                    assertThat(asDecimal(item.get("price"))).isEqualByComparingTo("0.00");
                });
    }

    @Test
    void shouldSaveOrderAsPendingConfirmationWhenInventoryServiceIsUnavailable() throws Exception {
        when(inventoryClient.reserve(any()))
                .thenThrow(new InventoryServiceUnavailableException(1L, new RuntimeException("circuit breaker open")));

        MvcResult response = postJson("/api/orders", order(new OrderItemRequest(1L, 2)));

        assertThat(status(response)).isEqualTo(201);
        Map<String, Object> created = readMap(response);
        assertThat(created.get("status"))
                .as("Неподтверждённый резерв не даёт статус CONFIRMED")
                .isEqualTo("PENDING_CONFIRMATION");
        assertThat(created.get("statusDetails").toString()).contains("ручной проверки");
        assertThat(asDecimal(created.get("totalPrice")))
                .as("Данные каталога получены, поэтому цена в снимке сохраняется")
                .isEqualByComparingTo("6980.00");
        verify(inventoryClient, never()).release(any());
    }

    @Test
    void shouldKeepSuccessfulReserveWhenOnlyPartOfWarehouseCallsDegraded() throws Exception {
        when(inventoryClient.reserve(new ReserveRequest(1L, 2)))
                .thenReturn(new ReserveResponse(true, 5, "ok"));
        when(inventoryClient.reserve(new ReserveRequest(2L, 1)))
                .thenThrow(new InventoryServiceUnavailableException(2L, new RuntimeException("timeout")));

        MvcResult response = postJson("/api/orders",
                order(new OrderItemRequest(1L, 2), new OrderItemRequest(2L, 1)));

        assertThat(status(response)).isEqualTo(201);
        assertThat(readMap(response).get("status")).isEqualTo("PENDING_CONFIRMATION");
        verify(inventoryClient, never()).release(any());
    }

    private MvcResult postJson(String url, Object body) throws Exception {
        return mvc.perform(post(url)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(body)))
                .andReturn();
    }

    private static int status(MvcResult result) {
        return result.getResponse().getStatus();
    }

    private Map<String, Object> readMap(MvcResult result) throws Exception {
        return json.readValue(result.getResponse().getContentAsByteArray(), new TypeReference<>() {
        });
    }

    private List<Map<String, Object>> readList(MvcResult result) throws Exception {
        return json.readValue(result.getResponse().getContentAsByteArray(), new TypeReference<>() {
        });
    }

    private static Long asLong(Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    private static BigDecimal asDecimal(Object value) {
        return new BigDecimal(value.toString());
    }
}
