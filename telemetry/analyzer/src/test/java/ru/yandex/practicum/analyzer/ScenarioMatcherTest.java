package ru.yandex.practicum.analyzer;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.analyzer.entity.Condition;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.service.ScenarioMatcher;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScenarioMatcherTest {

    private static final Instant TS = Instant.ofEpochMilli(1_000);

    private static SensorsSnapshotAvro snapshot(Map<String, Object> states) {
        Map<String, SensorStateAvro> sensorsState = new HashMap<>();
        states.forEach((id, data) -> sensorsState.put(id, new SensorStateAvro(TS, data)));
        return new SensorsSnapshotAvro("hub-1", TS, sensorsState);
    }

    private static Scenario scenario(Map<String, Condition> conditions) {
        Scenario scenario = new Scenario();
        scenario.setHubId("hub-1");
        scenario.setName("свет в прихожей");
        scenario.setConditions(new HashMap<>(conditions));
        return scenario;
    }

    private static Condition condition(ConditionTypeAvro type, ConditionOperationAvro operation, Integer value) {
        return new Condition(null, type, operation, value);
    }

    @Test
    void allConditionsMatched() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of(
                "light-1", new LightSensorAvro(88, 15),
                "motion-1", new MotionSensorAvro(79, true, 68)));
        Scenario scenario = scenario(Map.of(
                "light-1", condition(ConditionTypeAvro.LUMINOSITY, ConditionOperationAvro.LOWER_THAN, 20),
                "motion-1", condition(ConditionTypeAvro.MOTION, ConditionOperationAvro.EQUALS, 1)));

        assertTrue(ScenarioMatcher.matches(snapshot, scenario));
    }

    @Test
    void oneConditionFailedBreaksScenario() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of(
                "light-1", new LightSensorAvro(88, 40),
                "motion-1", new MotionSensorAvro(79, true, 68)));
        Scenario scenario = scenario(Map.of(
                "light-1", condition(ConditionTypeAvro.LUMINOSITY, ConditionOperationAvro.LOWER_THAN, 20),
                "motion-1", condition(ConditionTypeAvro.MOTION, ConditionOperationAvro.EQUALS, 1)));

        assertFalse(ScenarioMatcher.matches(snapshot, scenario));
    }

    @Test
    void missingSensorStateBreaksScenario() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of("light-1", new LightSensorAvro(88, 15)));
        Scenario scenario = scenario(Map.of(
                "light-1", condition(ConditionTypeAvro.LUMINOSITY, ConditionOperationAvro.LOWER_THAN, 20),
                "motion-1", condition(ConditionTypeAvro.MOTION, ConditionOperationAvro.EQUALS, 1)));

        assertFalse(ScenarioMatcher.matches(snapshot, scenario));
    }

    @Test
    void climateSensorSuppliesSeveralConditionTypes() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of("climate-1", new ClimateSensorAvro(7, 60, 335)));

        assertTrue(ScenarioMatcher.matches(snapshot, scenario(Map.of(
                "climate-1", condition(ConditionTypeAvro.TEMPERATURE, ConditionOperationAvro.LOWER_THAN, 15)))));
        assertTrue(ScenarioMatcher.matches(snapshot, scenario(Map.of(
                "climate-1", condition(ConditionTypeAvro.CO2LEVEL, ConditionOperationAvro.GREATER_THAN, 300)))));
        assertTrue(ScenarioMatcher.matches(snapshot, scenario(Map.of(
                "climate-1", condition(ConditionTypeAvro.HUMIDITY, ConditionOperationAvro.EQUALS, 60)))));
    }

    @Test
    void conditionTypeUnsupportedBySensorIsNotMatched() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of("light-1", new LightSensorAvro(88, 15)));

        assertFalse(ScenarioMatcher.matches(snapshot, scenario(Map.of(
                "light-1", condition(ConditionTypeAvro.TEMPERATURE, ConditionOperationAvro.LOWER_THAN, 20)))));
    }

    @Test
    void scenarioWithoutConditionsIsNotMatched() {
        SensorsSnapshotAvro snapshot = snapshot(Map.of("light-1", new LightSensorAvro(88, 15)));

        assertFalse(ScenarioMatcher.matches(snapshot, scenario(Map.of())));
    }
}
