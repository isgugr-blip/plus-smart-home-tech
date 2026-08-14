package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.dto.DeviceAction;
import ru.yandex.practicum.collector.dto.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.ScenarioCondition;
import ru.yandex.practicum.collector.dto.ScenarioRemovedEvent;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

public final class HubEventMapper {

    private HubEventMapper() {
    }

    public static HubEventAvro toAvro(HubEvent event) {
        HubEventAvro avro = new HubEventAvro();
        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp());
        avro.setPayload(toPayload(event));
        return avro;
    }

    private static Object toPayload(HubEvent event) {
        return switch (event.getType()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent e = (DeviceAddedEvent) event;
                DeviceAddedEventAvro payload = new DeviceAddedEventAvro();
                payload.setId(e.getId());
                payload.setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()));
                yield payload;
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent e = (DeviceRemovedEvent) event;
                DeviceRemovedEventAvro payload = new DeviceRemovedEventAvro();
                payload.setId(e.getId());
                yield payload;
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEvent e = (ScenarioAddedEvent) event;
                ScenarioAddedEventAvro payload = new ScenarioAddedEventAvro();
                payload.setName(e.getName());
                payload.setConditions(e.getConditions().stream()
                        .map(HubEventMapper::toConditionAvro)
                        .toList());
                payload.setActions(e.getActions().stream()
                        .map(HubEventMapper::toActionAvro)
                        .toList());
                yield payload;
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;
                ScenarioRemovedEventAvro payload = new ScenarioRemovedEventAvro();
                payload.setName(e.getName());
                yield payload;
            }
        };
    }

    private static ScenarioConditionAvro toConditionAvro(ScenarioCondition condition) {
        ScenarioConditionAvro avro = new ScenarioConditionAvro();
        avro.setSensorId(condition.getSensorId());
        avro.setType(ConditionTypeAvro.valueOf(condition.getType().name()));
        avro.setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));
        avro.setValue(condition.getValue());
        return avro;
    }

    private static DeviceActionAvro toActionAvro(DeviceAction action) {
        DeviceActionAvro avro = new DeviceActionAvro();
        avro.setSensorId(action.getSensorId());
        avro.setType(ActionTypeAvro.valueOf(action.getType().name()));
        avro.setValue(action.getValue());
        return avro;
    }
}
