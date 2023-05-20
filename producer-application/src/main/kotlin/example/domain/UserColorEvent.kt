package example.domain

import java.time.LocalDateTime

data class UserColorEvent(
    val timeStamp: LocalDateTime,
    val userAgent: String,
    val color: String,
    val name: String
)
