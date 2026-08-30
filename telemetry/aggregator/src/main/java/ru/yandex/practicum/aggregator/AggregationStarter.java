package ru.yandex.practicum.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class AggregationStarter {

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecordBase> producer;
    private final SnapshotAggregator aggregator;
    private final String sensorsTopic;
    private final String snapshotsTopic;
    private final Duration pollTimeout;

    public AggregationStarter(Consumer<String, SensorEventAvro> consumer,
                              Producer<String, SpecificRecordBase> producer,
                              SnapshotAggregator aggregator,
                              @Value("${aggregator.kafka.sensors-topic}") String sensorsTopic,
                              @Value("${aggregator.kafka.snapshots-topic}") String snapshotsTopic,
                              @Value("${aggregator.kafka.poll-timeout}") Duration pollTimeout) {
        this.consumer = consumer;
        this.producer = producer;
        this.aggregator = aggregator;
        this.sensorsTopic = sensorsTopic;
        this.snapshotsTopic = snapshotsTopic;
        this.pollTimeout = pollTimeout;
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(sensorsTopic));
            log.info("Подписались на топик [{}]", sensorsTopic);

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    aggregator.updateState(record.value()).ifPresent(this::send);
                }
                if (!records.isEmpty()) {
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private void send(SensorsSnapshotAvro snapshot) {
        SensorsSnapshotAvro copy = SensorsSnapshotAvro.newBuilder(snapshot).build();
        producer.send(new ProducerRecord<>(snapshotsTopic, copy.getHubId(), copy), (metadata, e) -> {
            if (e != null) {
                log.error("Не удалось отправить снимок хаба [{}]", snapshot.getHubId(), e);
            }
        });
    }
}
