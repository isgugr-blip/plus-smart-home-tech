package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;

public final class SensorEventMapper {

    private SensorEventMapper() {
    }

    public static SensorEventAvro toAvro(SensorEventProto event) {
        SensorEventAvro avro = new SensorEventAvro();
        avro.setId(event.getId());
        avro.setHubId(event.getHubId());
        avro.setTimestamp(toInstant(event));
        avro.setPayload(toPayload(event));
        return avro;
    }

    private static Instant toInstant(SensorEventProto event) {
        return Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
    }

    private static Object toPayload(SensorEventProto event) {
        return switch (event.getPayloadCase()) {
            case CLIMATE_SENSOR -> {
                var e = event.getClimateSensor();
                yield new ClimateSensorAvro(e.getTemperatureC(), e.getHumidity(), e.getCo2Level());
            }
            case LIGHT_SENSOR -> {
                var e = event.getLightSensor();
                yield new LightSensorAvro(e.getLinkQuality(), e.getLuminosity());
            }
            case MOTION_SENSOR -> {
                var e = event.getMotionSensor();
                yield new MotionSensorAvro(e.getLinkQuality(), e.getMotion(), e.getVoltage());
            }
            case SWITCH_SENSOR -> new SwitchSensorAvro(event.getSwitchSensor().getState());
            case TEMPERATURE_SENSOR -> {
                var e = event.getTemperatureSensor();
                yield new TemperatureSensorAvro(e.getTemperatureC(), e.getTemperatureF());
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload события датчика");
        };
    }
}
