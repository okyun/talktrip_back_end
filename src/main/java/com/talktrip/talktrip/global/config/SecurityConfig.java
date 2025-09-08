package com.talktrip.talktrip.global.config;

import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.global.security.filter.JWTCheckFilter;
import com.talktrip.talktrip.global.util.JWTUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JWTUtil jwtUtil;
    private final MemberRepository memberRepository;

    private static final String[] SWAGGER_WHITELIST = {
            "/swagger-ui.html", "/swagger-ui/**",
            "/v3/api-docs/**", "/swagger-resources/**",
            "/webjars/**", "/api-docs/**"
    };

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .cors()  // CORS 활성화
                .and()
                .csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(SWAGGER_WHITELIST).permitAll()
                        .requestMatchers("/api/products", "/api/products/**").permitAll()
                        .requestMatchers("/api/member/kakao-login-url").permitAll()
                        .requestMatchers("/api/member/kakao").permitAll()
                        .requestMatchers("/api/products", "/api/products/**", "/api/me/likes").permitAll()
                        .requestMatchers("/api/user/login").permitAll()
                        .requestMatchers("/api/orders/**").permitAll()
                        .requestMatchers("/api/tosspay/**").permitAll()
                        .requestMatchers("/api/chat/**").permitAll()  // 채팅 API 허용
                        .requestMatchers("/api/chat/me/**").permitAll()  // 채팅 API 허용 (더 구체적)
//                        .requestMatchers("/ws/**", "/ws").permitAll()
//                        .requestMatchers("/ws-info/**", "/ws-info").permitAll()
//                        .requestMatchers("/topic/**").permitAll()
//                        .requestMatchers("/app/**").permitAll()
//                        .requestMatchers("/ws/**", "/app/**", "/topic/**", "/websocket/**", "/sockjs-node/**").permitAll()

                        .anyRequest().authenticated()
                )
                .addFilterBefore(
                        new JWTCheckFilter(jwtUtil, memberRepository), UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.addAllowedOrigin("http://localhost:5173"); // ✅ 정확히 너가 쓰는 origin
        configuration.setAllowCredentials(true);                 // ✅ WebSocket은 반드시 true
        configuration.addAllowedMethod("*");
        configuration.addAllowedHeader("*");

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
