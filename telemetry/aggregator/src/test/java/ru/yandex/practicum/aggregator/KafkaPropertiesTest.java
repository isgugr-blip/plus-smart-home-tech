package ru.yandex.practicum.aggregator;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.yandex.practicum.aggregator.kafka.KafkaProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class KafkaPropertiesTest {

    @Autowired
    private KafkaProperties properties;

    @Test
    void bindsDottedKafkaKeys() {
        assertEquals("telemetry.sensors.v1", properties.sensorsTopic());
        assertEquals("false", properties.consumer().get("enable.auto.commit"));
        assertEquals("localhost:9092", properties.producer().get("bootstrap.servers"));
    }
}
