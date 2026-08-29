package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.*;

@GrpcService
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            SensorEventProto.PayloadCase payloadCase = request.getPayloadCase();
            switch (payloadCase) {
                case LIGHT_SENSOR:
                    System.out.println("Получено событие датчика освещённости");
                    LightSensorProto lightSensor = request.getLightSensor();
                    System.out.println("Уровень освещённости: " + lightSensor.getLuminosity());
                    break;
                case CLIMATE_SENSOR:
                    System.out.println("Получено событие климатического датчика");
                    ClimateSensorProto climateSensor = request.getClimateSensor();
                    System.out.println("Влажность воздуха: " + climateSensor.getHumidity());
                    break;
                case MOTION_SENSOR:
                    System.out.println("Получено событие датчика движения");
                    MotionSensorProto motionSensor = request.getMotionSensor();
                    System.out.println("Замечено движение: " + motionSensor.getMotion());
                    break;
                case SWITCH_SENSOR:
                    System.out.println("Получено событие датчика включения");
                    SwitchSensorProto switchSensorEvent = request.getSwitchSensor();
                    System.out.println("Включен: " + switchSensorEvent.getState());
                    break;
                case TEMPERATURE_SENSOR:
                    System.out.println("Получено событие датчика температуры");
                    TemperatureSensorProto temperatureSensorProto = request.getTemperatureSensor();
                    System.out.println("Температура: " + temperatureSensorProto.getTemperatureC());
                    break;
                default:
                    System.out.println("Получено событие неизвестного типа: " + payloadCase);
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            HubEventProto.PayloadCase payloadCase = request.getPayloadCase();
            System.out.println("Хаб: " + request.getHubId());
            switch (payloadCase) {
                case DEVICE_ADDED:
                    DeviceAddedEventProto deviceAdded = request.getDeviceAdded();
                    System.out.println("Добавлено устройство: " + deviceAdded.getId()
                            + ", тип: " + deviceAdded.getType());
                    break;
                case DEVICE_REMOVED:
                    System.out.println("Удалено устройство: " + request.getDeviceRemoved().getId());
                    break;
                case SCENARIO_ADDED:
                    ScenarioAddedEventProto scenarioAdded = request.getScenarioAdded();
                    System.out.println("Добавлен сценарий: " + scenarioAdded.getName()
                            + ", условий: " + scenarioAdded.getConditionsCount()
                            + ", действий: " + scenarioAdded.getActionsCount());
                    break;
                case SCENARIO_REMOVED:
                    System.out.println("Удалён сценарий: " + request.getScenarioRemoved().getName());
                    break;
                default:
                    System.out.println("Получено событие неизвестного типа: " + payloadCase);
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
