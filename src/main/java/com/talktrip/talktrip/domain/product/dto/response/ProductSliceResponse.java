package com.talktrip.talktrip.domain.product.dto.response;

import java.util.List;

public record ProductSliceResponse(List<ProductSummaryResponse> items, boolean hasNext, String nextCursor) {}


