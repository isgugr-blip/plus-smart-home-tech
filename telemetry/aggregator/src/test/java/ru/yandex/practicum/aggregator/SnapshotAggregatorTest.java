package ru.yandex.practicum.aggregator;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotAggregatorTest {

    private static final Instant TS = Instant.ofEpochMilli(1_000);

    private final SnapshotAggregator aggregator = new SnapshotAggregator();

    private static SensorEventAvro event(String hubId, String id, Instant timestamp, int luminosity) {
        return SensorEventAvro.newBuilder()
                .setId(id)
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(10)
                        .setLuminosity(luminosity)
                        .build())
                .build();
    }

    @Test
    void firstEventCreatesSnapshot() {
        Optional<SensorsSnapshotAvro> snapshot = aggregator.updateState(event("hub-1", "light-1", TS, 15));

        assertTrue(snapshot.isPresent());
        assertEquals(TS, snapshot.get().getTimestamp());
        assertEquals(1, snapshot.get().getSensorsState().size());
    }

    @Test
    void changedDataUpdatesSnapshot() {
        aggregator.updateState(event("hub-1", "light-1", TS, 15));

        Optional<SensorsSnapshotAvro> snapshot = aggregator.updateState(event("hub-1", "light-1", TS.plusMillis(1), 25));

        assertTrue(snapshot.isPresent());
        assertEquals(TS.plusMillis(1), snapshot.get().getSensorsState().get("light-1").getTimestamp());
    }

    @Test
    void sameDataIsIgnored() {
        aggregator.updateState(event("hub-1", "light-1", TS, 15));

        assertTrue(aggregator.updateState(event("hub-1", "light-1", TS.plusMillis(1), 15)).isEmpty());
    }

    @Test
    void outdatedEventIsIgnored() {
        aggregator.updateState(event("hub-1", "light-1", TS, 15));

        assertTrue(aggregator.updateState(event("hub-1", "light-1", TS.minusMillis(1), 25)).isEmpty());
    }

    @Test
    void hubsAreAggregatedSeparately() {
        aggregator.updateState(event("hub-1", "light-1", TS, 15));

        Optional<SensorsSnapshotAvro> snapshot = aggregator.updateState(event("hub-2", "light-1", TS, 15));

        assertTrue(snapshot.isPresent());
        assertEquals("hub-2", snapshot.get().getHubId());
        assertEquals(1, snapshot.get().getSensorsState().size());
    }
}
