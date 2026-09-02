package ru.yandex.practicum.analyzer.service;

import ru.yandex.practicum.analyzer.entity.Condition;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

public final class ScenarioMatcher {

    private ScenarioMatcher() {
    }

    public static boolean matches(SensorsSnapshotAvro snapshot, Scenario scenario) {
        return !scenario.getConditions().isEmpty()
                && scenario.getConditions().entrySet().stream()
                .allMatch(entry -> matches(snapshot.getSensorsState().get(entry.getKey()), entry.getValue()));
    }

    private static boolean matches(SensorStateAvro state, Condition condition) {
        if (state == null || condition.getValue() == null) {
            return false;
        }
        Integer actual = valueOf(condition.getType(), state.getData());
        if (actual == null) {
            return false;
        }
        return switch (condition.getOperation()) {
            case EQUALS -> actual.intValue() == condition.getValue();
            case GREATER_THAN -> actual > condition.getValue();
            case LOWER_THAN -> actual < condition.getValue();
        };
    }

    private static Integer valueOf(ConditionTypeAvro type, Object data) {
        return switch (data) {
            case ClimateSensorAvro climate -> switch (type) {
                case TEMPERATURE -> climate.getTemperatureC();
                case CO2LEVEL -> climate.getCo2Level();
                case HUMIDITY -> climate.getHumidity();
                default -> null;
            };
            case TemperatureSensorAvro temperature ->
                    type == ConditionTypeAvro.TEMPERATURE ? temperature.getTemperatureC() : null;
            case LightSensorAvro light ->
                    type == ConditionTypeAvro.LUMINOSITY ? light.getLuminosity() : null;
            case MotionSensorAvro motion ->
                    type == ConditionTypeAvro.MOTION ? flag(motion.getMotion()) : null;
            case SwitchSensorAvro switchSensor ->
                    type == ConditionTypeAvro.SWITCH ? flag(switchSensor.getState()) : null;
            case null, default -> null;
        };
    }

    private static Integer flag(boolean value) {
        return value ? 1 : 0;
    }
}
