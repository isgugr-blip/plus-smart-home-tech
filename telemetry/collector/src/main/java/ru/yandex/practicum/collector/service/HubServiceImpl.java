package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.kafka.KafkaClient;
import ru.yandex.practicum.collector.kafka.KafkaTopics;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubServiceImpl implements HubService {

    private final KafkaClient kafkaClient;

    @Override
    public void send(HubEventProto event) {
        HubEventAvro avro = HubEventMapper.toAvro(event);
        log.info("Событие хаба {} (hubId={}) отправляется в топик {}",
                event.getPayloadCase(), event.getHubId(), KafkaTopics.HUBS);
        kafkaClient.getProducer().send(new ProducerRecord<>(
                KafkaTopics.HUBS,
                null,
                avro.getTimestamp().toEpochMilli(),
                avro.getHubId(),
                avro));
    }
}
