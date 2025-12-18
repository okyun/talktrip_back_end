package com.talktrip.talktrip.domain.kafka.dto.product;

import java.time.Instant;

public record ProductClickStatResponse(
        String productId,
        long clickCount,
        Instant windowStart,
        Instant windowEnd
) {
}

