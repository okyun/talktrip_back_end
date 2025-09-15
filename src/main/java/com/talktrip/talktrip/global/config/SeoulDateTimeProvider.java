package com.talktrip.talktrip.global.config;

import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.stereotype.Component;

import java.time.temporal.TemporalAccessor;
import java.util.Optional;

/**
 * JPA Auditing에서 서울 시간대를 사용하도록 하는 DateTimeProvider
 * 
 * @CreatedDate와 @LastModifiedDate가 서울 시간대를 사용하도록 설정
 */
@Component
public class SeoulDateTimeProvider implements DateTimeProvider {

    @Override
    public Optional<TemporalAccessor> getNow() {
        return Optional.of(SeoulTimeUtil.now());
    }
}
