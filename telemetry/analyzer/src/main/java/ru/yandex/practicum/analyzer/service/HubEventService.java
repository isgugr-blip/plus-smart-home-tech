package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.entity.Action;
import ru.yandex.practicum.analyzer.entity.Condition;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.entity.Sensor;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;

    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();
        switch (event.getPayload()) {
            case DeviceAddedEventAvro payload -> addSensor(hubId, payload);
            case DeviceRemovedEventAvro payload -> removeSensor(hubId, payload);
            case ScenarioAddedEventAvro payload -> addScenario(hubId, payload);
            case ScenarioRemovedEventAvro payload -> removeScenario(hubId, payload);
            case null, default -> log.warn("Неизвестное событие хаба [{}]", event.getPayload());
        }
    }

    private void addSensor(String hubId, DeviceAddedEventAvro payload) {
        if (sensorRepository.existsById(payload.getId())) {
            log.debug("Датчик [{}] хаба [{}] уже зарегистрирован", payload.getId(), hubId);
            return;
        }
        sensorRepository.save(new Sensor(payload.getId(), hubId));
        log.info("Зарегистрирован датчик [{}] хаба [{}]", payload.getId(), hubId);
    }

    private void removeSensor(String hubId, DeviceRemovedEventAvro payload) {
        sensorRepository.findByIdAndHubId(payload.getId(), hubId).ifPresent(sensor -> {
            List<Scenario> broken = scenarioRepository.findByHubId(hubId).stream()
                    .filter(scenario -> scenario.getConditions().containsKey(sensor.getId())
                            || scenario.getActions().containsKey(sensor.getId()))
                    .toList();
            if (!broken.isEmpty()) {
                scenarioRepository.deleteAll(broken);
                scenarioRepository.flush();
                log.info("Удалены сценарии {} хаба [{}], зависевшие от датчика [{}]",
                        broken.stream().map(Scenario::getName).toList(), hubId, sensor.getId());
            }
            sensorRepository.delete(sensor);
            log.info("Удалён датчик [{}] хаба [{}]", payload.getId(), hubId);
        });
    }

    private void addScenario(String hubId, ScenarioAddedEventAvro payload) {
        List<String> sensorIds = Stream.concat(
                        payload.getConditions().stream().map(ScenarioConditionAvro::getSensorId),
                        payload.getActions().stream().map(DeviceActionAvro::getSensorId))
                .distinct()
                .toList();
        if (sensorRepository.countByIdInAndHubId(sensorIds, hubId) != sensorIds.size()) {
            log.warn("Сценарий [{}] ссылается на датчики, не зарегистрированные в хабе [{}]", payload.getName(), hubId);
            return;
        }

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, payload.getName())
                .orElseGet(() -> {
                    Scenario created = new Scenario();
                    created.setHubId(hubId);
                    created.setName(payload.getName());
                    return created;
                });

        scenario.getConditions().clear();
        payload.getConditions().forEach(condition ->
                scenario.getConditions().put(condition.getSensorId(), toCondition(condition)));
        scenario.getActions().clear();
        payload.getActions().forEach(action ->
                scenario.getActions().put(action.getSensorId(), toAction(action)));

        scenarioRepository.save(scenario);
        log.info("Сохранён сценарий [{}] хаба [{}]", payload.getName(), hubId);
    }

    private void removeScenario(String hubId, ScenarioRemovedEventAvro payload) {
        scenarioRepository.findByHubIdAndName(hubId, payload.getName()).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            log.info("Удалён сценарий [{}] хаба [{}]", payload.getName(), hubId);
        });
    }

    private static Condition toCondition(ScenarioConditionAvro condition) {
        return new Condition(null, condition.getType(), condition.getOperation(), toValue(condition.getValue()));
    }

    private static Action toAction(DeviceActionAvro action) {
        return new Action(null, action.getType(), action.getValue());
    }

    private static Integer toValue(Object value) {
        return switch (value) {
            case Boolean bool -> bool ? 1 : 0;
            case Integer number -> number;
            case null, default -> null;
        };
    }
}
