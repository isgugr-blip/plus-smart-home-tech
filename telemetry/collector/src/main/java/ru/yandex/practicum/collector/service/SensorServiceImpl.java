package ru.yandex.practicum.collector.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.collector.kafka.KafkaTopics;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorServiceImpl implements SensorService {

    private final KafkaClient kafkaClient;

    @Override
    public void send(SensorEvent event) {
        SensorEventAvro avro = SensorEventMapper.toAvro(event);
        log.info("Событие датчика {} (id={}) отправляется в топик {}",
                event.getType(), event.getId(), KafkaTopics.SENSORS);
        kafkaClient.getProducer().send(new ProducerRecord<>(
                KafkaTopics.SENSORS,
                null,
                event.getTimestamp().toEpochMilli(),
                event.getHubId(),
                avro));
    }

    @PreDestroy
    void shutdown() {
        kafkaClient.stop();
    }
}
