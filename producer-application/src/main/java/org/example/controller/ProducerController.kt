package org.example.controller

import org.example.domain.ProducerService
import org.example.domain.UserColorEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class ProducerController(
    val producerService: ProducerService
) {
    @GetMapping("/api/select")
    fun selectColor(
        userColorEvent: UserColorEvent
    ) {
        producerService.send(USER_COLOR_TOPIC, userColorEvent.toString())
    }

    companion object {
        const val USER_COLOR_TOPIC = "select-color"
    }
}