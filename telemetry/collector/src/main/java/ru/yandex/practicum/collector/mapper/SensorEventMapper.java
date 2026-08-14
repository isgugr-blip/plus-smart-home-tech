package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.dto.ClimateSensorEvent;
import ru.yandex.practicum.collector.dto.LightSensorEvent;
import ru.yandex.practicum.collector.dto.MotionSensorEvent;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SwitchSensorEvent;
import ru.yandex.practicum.collector.dto.TemperatureSensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

public final class SensorEventMapper {

    private SensorEventMapper() {
    }

    public static SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro avro = new SensorEventAvro();
        avro.setId(event.getId());
        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp());
        avro.setPayload(toPayload(event));
        return avro;
    }

    private static Object toPayload(SensorEvent event) {
        return switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent e = (ClimateSensorEvent) event;
                ClimateSensorAvro payload = new ClimateSensorAvro();
                payload.setTemperatureC(e.getTemperatureC());
                payload.setHumidity(e.getHumidity());
                payload.setCo2Level(e.getCo2Level());
                yield payload;
            }
            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent e = (LightSensorEvent) event;
                LightSensorAvro payload = new LightSensorAvro();
                payload.setLinkQuality(e.getLinkQuality());
                payload.setLuminosity(e.getLuminosity());
                yield payload;
            }
            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent e = (MotionSensorEvent) event;
                MotionSensorAvro payload = new MotionSensorAvro();
                payload.setLinkQuality(e.getLinkQuality());
                payload.setMotion(e.isMotion());
                payload.setVoltage(e.getVoltage());
                yield payload;
            }
            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent e = (SwitchSensorEvent) event;
                SwitchSensorAvro payload = new SwitchSensorAvro();
                payload.setState(e.isState());
                yield payload;
            }
            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent e = (TemperatureSensorEvent) event;
                TemperatureSensorAvro payload = new TemperatureSensorAvro();
                payload.setId(e.getId());
                payload.setHubId(e.getHubId());
                payload.setTimestamp(e.getTimestamp());
                payload.setTemperatureC(e.getTemperatureC());
                payload.setTemperatureF(e.getTemperatureF());
                yield payload;
            }
        };
    }
}
