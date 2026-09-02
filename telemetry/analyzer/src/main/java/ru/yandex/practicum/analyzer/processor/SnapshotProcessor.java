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
import ru.yandex.practicum.analyzer.service.SnapshotService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotService snapshotService;
    private final KafkaProperties properties;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public SnapshotProcessor(Consumer<String, SensorsSnapshotAvro> snapshotConsumer,
                             SnapshotService snapshotService,
                             KafkaProperties properties) {
        this.consumer = snapshotConsumer;
        this.snapshotService = snapshotService;
        this.properties = properties;
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(properties.snapshotsTopic()));
            log.info("Подписались на топик [{}]", properties.snapshotsTopic());

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(properties.pollTimeout());
                boolean processed = false;
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        snapshotService.handle(record.value());
                    } catch (Exception e) {
                        log.error("Не удалось обработать снимок хаба [{}], смещение не фиксируем",
                                record.value().getHubId(), e);
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
            log.error("Ошибка во время обработки снимков состояния", e);
        } finally {
            try {
                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            } finally {
                log.info("Закрываем консьюмер снимков состояния");
                consumer.close();
            }
        }
    }
}
