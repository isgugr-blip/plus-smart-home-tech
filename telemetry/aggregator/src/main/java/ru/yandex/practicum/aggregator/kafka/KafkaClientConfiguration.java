package ru.yandex.practicum.aggregator.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.HashMap;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaClientConfiguration {

    @Bean(destroyMethod = "")
    Consumer<String, SensorEventAvro> sensorEventConsumer(KafkaProperties properties) {
        return new KafkaConsumer<>(new HashMap<String, Object>(properties.consumer()));
    }

    @Bean(destroyMethod = "")
    Producer<String, SpecificRecordBase> snapshotProducer(KafkaProperties properties) {
        return new KafkaProducer<>(new HashMap<String, Object>(properties.producer()));
    }
}
