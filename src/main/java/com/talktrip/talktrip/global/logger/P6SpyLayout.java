package com.talktrip.talktrip.global.logger;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.regex.Pattern;

/**
 * p6spy 로그 메시지를 후처리하여 SQL에 파라미터를 치환하는 커스텀 Layout
 */
public class P6SpyLayout extends PatternLayout {
    
    private static final Pattern SQL_PATTERN = Pattern.compile("(?i)(SELECT|INSERT|UPDATE|DELETE|CALL|EXEC|EXECUTE)");
    
    @Override
    public String doLayout(ILoggingEvent event) {
        String formattedMessage = super.doLayout(event);
        
        // p6spy 로거이고 SQL이 포함된 메시지인 경우 파라미터 치환
        if (event.getLoggerName() != null && event.getLoggerName().equals("p6spy")) {
            if (formattedMessage != null && formattedMessage.contains("?") && 
                SQL_PATTERN.matcher(formattedMessage).find()) {
                
                // 파라미터 바인딩 정보가 ThreadLocal에 저장되어 있을 때 치환
                String replaced = SqlParameterBinder.replaceParameters(formattedMessage);
                if (replaced != null && !replaced.equals(formattedMessage) && !replaced.contains("?")) {
                    formattedMessage = replaced;
                    // 사용 후 정리 (다음 쿼리를 위해)
                    SqlParameterBinder.clear();
                }
            }
        }
        
        return formattedMessage;
    }
}

