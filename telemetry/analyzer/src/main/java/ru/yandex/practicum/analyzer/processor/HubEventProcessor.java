package ru.yandex.practicum.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.kafka.KafkaProperties;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventService hubEventService;
    private final KafkaProperties properties;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public HubEventProcessor(Consumer<String, HubEventAvro> hubEventConsumer,
                             HubEventService hubEventService,
                             KafkaProperties properties) {
        this.consumer = hubEventConsumer;
        this.hubEventService = hubEventService;
        this.properties = properties;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(properties.hubsTopic()));
            log.info("Подписались на топик [{}]", properties.hubsTopic());

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(properties.pollTimeout());
                boolean processed = false;
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        hubEventService.handle(record.value());
                    } catch (Exception e) {
                        log.error("Не удалось обработать событие хаба {}, смещение не фиксируем", record.value(), e);
                        break;
                    }
                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    processed = true;
                }
                if (processed) {
                    consumer.commitAsync(offsets, (committed, error) -> {
                        if (error != null) {
                            log.warn("Не удалось зафиксировать смещения {}", committed, error);
                        }
                    });
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хабов", e);
        } finally {
            try {
                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            } finally {
                log.info("Закрываем консьюмер событий хабов");
                consumer.close();
            }
        }
    }
}
