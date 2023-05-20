package org.example.configuration.resolver

import jakarta.servlet.http.HttpServletRequest
import org.example.domain.UserColorEvent
import org.springframework.core.MethodParameter
import org.springframework.stereotype.Component
import org.springframework.web.bind.support.WebDataBinderFactory
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.web.method.support.HandlerMethodArgumentResolver
import org.springframework.web.method.support.ModelAndViewContainer
import java.time.LocalDateTime

@Component
class UserColorEventResolver: HandlerMethodArgumentResolver {
    override fun supportsParameter(parameter: MethodParameter): Boolean {
        return parameter.parameterType == UserColorEvent::class.java
    }

    override fun resolveArgument(
        parameter: MethodParameter,
        mavContainer: ModelAndViewContainer?,
        webRequest: NativeWebRequest,
        binderFactory: WebDataBinderFactory?
    ): Any? {
        val request = webRequest.nativeRequest as HttpServletRequest
        return UserColorEvent(
            timeStamp = LocalDateTime.now(),
            userAgent = request.getHeader("user-agent"),
            color = request.getParameter("color"),
            name = request.getParameter("user")
        )
    }
}