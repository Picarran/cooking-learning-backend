package com.example.cooking.controller;

import com.example.cooking.dao.entity.User;
import com.example.cooking.dto.req.WxLoginReq;
import com.example.cooking.dto.resp.WxLoginResp;
import com.example.cooking.utils.JwtUtil;
import lombok.RequiredArgsConstructor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.cooking.dto.resp.Result;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final com.example.cooking.service.UserService userService;
    private final JwtUtil jwtUtil;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // token ttl: 5 hour
    private static final long TOKEN_TTL = 60L * 60L * 1000L * 5L;

    @PostMapping("/wxlogin")
    public Result<?> wxlogin(WxLoginReq req) {
        if (!StringUtils.hasText(req.getCode())) {
            return Result.buildFailure(Map.of("msg","code required"));
        }

        // prefer server side appId/secret if not provided
        String appId = req.getAppId();
        String secret = req.getSecret();

        String url = String.format("https://api.weixin.qq.com/sns/jscode2session?appid=%s&secret=%s&js_code=%s&grant_type=authorization_code",
                appId, secret, req.getCode());

        String raw = restTemplate.getForObject(url, String.class);
        Map<String, Object> wxResp = null;
        try {
            wxResp = objectMapper.readValue(raw, new TypeReference<Map<String, Object>>(){});
        } catch (Exception e) {
            return Result.buildFailure(Map.of("msg","wx login failed - invalid response","detail", raw));
        }
        if (wxResp == null || wxResp.get("openid") == null) {
            return Result.buildFailure(Map.of("msg","wx login failed","detail",wxResp));
        }

        String openid = String.valueOf(wxResp.get("openid"));

        User user = userService.findOrCreateOrUpdateByOpenid(openid);

        String token = jwtUtil.generateToken(user.getId(), user.getOpenid(), TOKEN_TTL);

        WxLoginResp resp = WxLoginResp.builder()
                .id(user.getId())
                .avatarUrl(user.getAvatarUrl())
                .nickname(user.getNickname())
                .points(user.getPoints())
                .token(token)
                .build();

        return Result.buildSuccess(resp);
    }
}
