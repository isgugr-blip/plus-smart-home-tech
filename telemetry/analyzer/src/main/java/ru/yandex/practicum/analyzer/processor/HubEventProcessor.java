package ru.yandex.practicum.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventService hubEventService;
    private final String topic;
    private final Duration pollTimeout;

    public HubEventProcessor(Consumer<String, HubEventAvro> hubEventConsumer,
                             HubEventService hubEventService,
                             @Value("${analyzer.kafka.hubs-topic}") String topic,
                             @Value("${analyzer.kafka.poll-timeout}") Duration pollTimeout) {
        this.consumer = hubEventConsumer;
        this.hubEventService = hubEventService;
        this.topic = topic;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
        try {
            consumer.subscribe(List.of(topic));
            log.info("Подписались на топик [{}]", topic);

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        hubEventService.handle(record.value());
                    } catch (Exception e) {
                        log.error("Не удалось обработать событие хаба {}", record.value(), e);
                    }
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хабов", e);
        } finally {
            log.info("Закрываем консьюмер событий хабов");
            consumer.close();
        }
    }
}
