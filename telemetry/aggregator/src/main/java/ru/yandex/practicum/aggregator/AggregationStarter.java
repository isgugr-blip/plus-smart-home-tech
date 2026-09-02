package ru.yandex.practicum.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.kafka.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class AggregationStarter {

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecordBase> producer;
    private final SnapshotAggregator aggregator;
    private final KafkaProperties properties;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public AggregationStarter(Consumer<String, SensorEventAvro> consumer,
                              Producer<String, SpecificRecordBase> producer,
                              SnapshotAggregator aggregator,
                              KafkaProperties properties) {
        this.consumer = consumer;
        this.producer = producer;
        this.aggregator = aggregator;
        this.properties = properties;
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(properties.sensorsTopic()));
            log.info("Подписались на топик [{}]", properties.sensorsTopic());

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(properties.pollTimeout());
                boolean processed = false;
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    try {
                        aggregator.updateState(record.value()).ifPresent(this::send);
                    } catch (Exception e) {
                        log.error("Не удалось обработать событие датчика [{}], смещение не фиксируем",
                                record.value().getId(), e);
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
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private void send(SensorsSnapshotAvro snapshot) {
        try {
            producer.send(new ProducerRecord<>(properties.snapshotsTopic(), snapshot.getHubId(), snapshot)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Не удалось отправить снимок хаба " + snapshot.getHubId(), e);
        }
    }
}
