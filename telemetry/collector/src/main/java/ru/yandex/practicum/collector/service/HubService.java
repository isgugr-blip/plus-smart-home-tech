package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

public interface HubService {
    void send(HubEventProto event);
}
