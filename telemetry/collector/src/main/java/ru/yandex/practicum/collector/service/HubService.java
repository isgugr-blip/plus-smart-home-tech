package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.dto.HubEvent;

public interface HubService {
    void send(HubEvent event);
}
