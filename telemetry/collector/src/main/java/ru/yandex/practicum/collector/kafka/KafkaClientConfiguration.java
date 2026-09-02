package ru.yandex.practicum.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.HashMap;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaClientConfiguration {

    @Bean(destroyMethod = "stop")
    KafkaClient kafkaClient(KafkaProperties properties) {
        return new KafkaClient() {

            private Producer<String, SpecificRecordBase> producer;

            @Override
            public Producer<String, SpecificRecordBase> getProducer() {
                if (producer == null) {
                    producer = new KafkaProducer<>(new HashMap<String, Object>(properties.producer()));
                }
                return producer;
            }

            @Override
            public void stop() {
                if (producer != null) {
                    producer.flush();
                    producer.close(Duration.ofSeconds(10));
                }
            }
        };
    }
}
