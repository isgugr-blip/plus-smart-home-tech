package ru.yandex.practicum.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.entity.Action;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;

@Slf4j
@Service
public class SnapshotService {

    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerBlockingStub hubRouterClient;

    public SnapshotService(ScenarioRepository scenarioRepository,
                           @GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouterClient) {
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro snapshot) {
        scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> ScenarioMatcher.matches(snapshot, scenario))
                .forEach(scenario -> execute(snapshot.getHubId(), scenario));
    }

    private void execute(String hubId, Scenario scenario) {
        log.info("Сработал сценарий [{}] хаба [{}]", scenario.getName(), hubId);
        scenario.getActions().forEach((sensorId, action) ->
                hubRouterClient.handleDeviceAction(request(hubId, scenario.getName(), sensorId, action)));
    }

    private static DeviceActionRequest request(String hubId, String scenarioName, String sensorId, Action action) {
        DeviceActionProto.Builder deviceAction = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action.getType().name()));
        if (action.getValue() != null) {
            deviceAction.setValue(action.getValue());
        }
        Instant now = Instant.now();
        return DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(deviceAction)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()))
                .build();
    }
}
