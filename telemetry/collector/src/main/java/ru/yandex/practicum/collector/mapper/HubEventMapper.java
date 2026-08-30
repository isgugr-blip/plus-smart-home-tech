package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
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

import java.time.Instant;

public final class HubEventMapper {

    private HubEventMapper() {
    }

    public static HubEventAvro toAvro(HubEventProto event) {
        HubEventAvro avro = new HubEventAvro();
        avro.setHubId(event.getHubId());
        avro.setTimestamp(Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()));
        avro.setPayload(toPayload(event));
        return avro;
    }

    private static Object toPayload(HubEventProto event) {
        return switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> {
                var e = event.getDeviceAdded();
                yield new DeviceAddedEventAvro(e.getId(), DeviceTypeAvro.valueOf(e.getType().name()));
            }
            case DEVICE_REMOVED -> new DeviceRemovedEventAvro(event.getDeviceRemoved().getId());
            case SCENARIO_ADDED -> {
                var e = event.getScenarioAdded();
                yield new ScenarioAddedEventAvro(
                        e.getName(),
                        e.getConditionsList().stream().map(HubEventMapper::toConditionAvro).toList(),
                        e.getActionsList().stream().map(HubEventMapper::toActionAvro).toList());
            }
            case SCENARIO_REMOVED -> new ScenarioRemovedEventAvro(event.getScenarioRemoved().getName());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload события хаба");
        };
    }

    private static ScenarioConditionAvro toConditionAvro(ScenarioConditionProto condition) {
        Object value = switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
        };
        return new ScenarioConditionAvro(
                condition.getSensorId(),
                ConditionTypeAvro.valueOf(condition.getType().name()),
                ConditionOperationAvro.valueOf(condition.getOperation().name()),
                value);
    }

    private static DeviceActionAvro toActionAvro(DeviceActionProto action) {
        return new DeviceActionAvro(
                action.getSensorId(),
                ActionTypeAvro.valueOf(action.getType().name()),
                action.hasValue() ? action.getValue() : null);
    }
}
