package ru.yandex.practicum.aggregator;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class SnapshotAggregator {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(event.getHubId(), hubId ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build());

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        if (oldState != null
                && (oldState.getTimestamp().isAfter(event.getTimestamp())
                || oldState.getData().equals(event.getPayload()))) {
            return Optional.empty();
        }

        snapshot.getSensorsState().put(event.getId(), SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build());
        snapshot.setTimestamp(event.getTimestamp());
        return Optional.of(snapshot);
    }
}
