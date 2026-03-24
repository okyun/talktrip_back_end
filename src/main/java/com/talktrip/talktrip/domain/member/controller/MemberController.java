package com.talktrip.talktrip.domain.member.controller;

import com.talktrip.talktrip.domain.member.dto.request.MemberUpdateRequestDTO;
import com.talktrip.talktrip.domain.member.dto.response.MemberProfileView;
import com.talktrip.talktrip.domain.member.dto.response.MemberResponseDTO;
import com.talktrip.talktrip.domain.member.service.KakaoAuthService;
import com.talktrip.talktrip.domain.member.service.MemberService;
import com.talktrip.talktrip.global.security.CustomMemberDetails;
import com.talktrip.talktrip.global.util.JWTUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@Tag(name = "Member", description = "회원 관련 API")
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MemberController {

    private final KakaoAuthService kakaoAuthService;
    private final MemberService memberService;

    @Operation(summary = "카카오 로그인 URL 요청", description = "카카오 로그인 인가 URL을 반환합니다.")
    @GetMapping("/member/kakao-login-url")
    public ResponseEntity<?> getKakaoLoginUrl() {
        String kakaoUrl = kakaoAuthService.getKakaoAuthorizeUrl();
        return ResponseEntity.ok(Map.of("url", kakaoUrl));
    }


    @Operation(summary = "카카오 로그인 콜백", description = "인가 코드를 통해 로그인 처리를 수행합니다.")
    @PostMapping("/member/kakao")
    public Map<String, Object> kakaoLogin(String code) {

        MemberResponseDTO memberDTO = kakaoAuthService.loginWithKakao(code);

        Map<String, Object> claims = memberDTO.getClaims();

        String accessToken = JWTUtil.generateToken(claims, 60 * 24);
        String refreshToken = JWTUtil.generateToken(claims, 60 * 24 * 30);

        claims.put("accessToken", accessToken);
        claims.put("refreshToken", refreshToken);

        return claims;
    }

    @Operation(summary = "회원 공개 프로필 (캐시)", description = "닉네임·이름·프로필 이미지 URL. 채팅/카드 UI용.")
    @GetMapping("/member/profile/{memberId}")
    public ResponseEntity<MemberProfileView> getMemberProfile(@PathVariable Long memberId) {
        return memberService.getMemberProfileView(memberId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(summary = "내 정보 조회", description = "로그인한 사용자의 정보를 반환합니다.")
    @GetMapping("/member/me")
    public ResponseEntity<?> getMyInfo(@AuthenticationPrincipal CustomMemberDetails memberDetails) {
        if (memberDetails == null) {
            return ResponseEntity.status(401).body("인증되지 않은 사용자입니다.");
        }

        Long memberId = memberDetails.getId();
        MemberResponseDTO myInfo = memberService.getMemberInfo(memberId);

        return ResponseEntity.ok(myInfo);
    }

    @Operation(summary = "내 정보 수정", description = "로그인한 사용자의 정보를 수정합니다.")
    @PutMapping(value = "/member/me", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> updateMyInfo(
            @AuthenticationPrincipal CustomMemberDetails memberDetails,
            @RequestPart("info") MemberUpdateRequestDTO updateDTO,
            @RequestPart(value = "profile_image", required = false) MultipartFile profileImage
    ) {
        if (memberDetails == null) {
            return ResponseEntity.status(401).body("인증되지 않은 사용자입니다.");
        }

        Long memberId = memberDetails.getId();
        memberService.updateMemberProfile(memberId, updateDTO, profileImage);
        return ResponseEntity.ok("프로필이 성공적으로 수정되었습니다.");
    }

}