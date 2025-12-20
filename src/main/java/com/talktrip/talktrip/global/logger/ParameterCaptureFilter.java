package com.talktrip.talktrip.global.logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hibernate 파라미터 바인딩 로그를 캡처하여 SqlParameterBinder에 저장
 */
public class ParameterCaptureFilter extends Filter<ILoggingEvent> {
    
    private static final Pattern BINDING_PATTERN = 
        Pattern.compile("binding parameter \\((\\d+):([^)]+)\\) <- \\[(.*?)\\]");
    
    @Override
    public FilterReply decide(ILoggingEvent event) {
        String loggerName = event.getLoggerName();
        
        // Hibernate의 파라미터 바인딩 로그를 캡처
        if (loggerName != null && loggerName.contains("org.hibernate.orm.jdbc.bind")) {
            String message = event.getFormattedMessage();
            if (message != null) {
                Matcher matcher = BINDING_PATTERN.matcher(message);
                if (matcher.find()) {
                    try {
                        int index = Integer.parseInt(matcher.group(1));
                        String type = matcher.group(2);
                        String value = matcher.group(3);
                        SqlParameterBinder.addParameter(index, type, value);
                        // 파라미터 바인딩 로그는 출력 (캡처와 함께 출력)
                        return FilterReply.NEUTRAL;
                    } catch (Exception e) {
                        // 파싱 실패 시 무시
                    }
                }
            }
        }
        
        
        // Hibernate SQL 로그는 출력하지 않음 (p6spy에서 처리)
        if (loggerName != null && loggerName.contains("org.hibernate.SQL")) {
            return FilterReply.DENY;
        }
        
        // 나머지 로그는 통과
        return FilterReply.NEUTRAL;
    }
}

