package com.talktrip.talktrip.domain.member.dto.response;

import com.talktrip.talktrip.domain.member.entity.Member;

/**
 * 채팅방 목록·상품 카드 등에 쓰는 공개 프로필 스냅샷 (Redis user 캐시용, 민감도 낮은 필드만).
 */
public record MemberProfileView(
        Long id,
        String nickname,
        String name,
        String profileImage
) {
    public static MemberProfileView from(Member member) {
        return new MemberProfileView(
                member.getId(),
                member.getNickname(),
                member.getName(),
                member.getProfileImage()
        );
    }
}
