package com.example.cooking.controller;

import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.User;
import com.example.cooking.dto.req.UpdateUserReq;
import com.example.cooking.dto.resp.Result;
import com.example.cooking.dto.resp.UserProfileResp;
import com.example.cooking.service.UserService;
import com.example.cooking.utils.UserContext;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @GetMapping("/me")
    public Result<UserProfileResp> getMe() {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();

        return Result.buildSuccess(userService.getById(uid));
    }

    @PutMapping("/me")
    public Result<?> updateMe(UpdateUserReq req) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();

        UserProfileResp updated = userService.updateProfile(uid, req.getNickname(), req.getAvatarUrl());
        return Result.buildSuccess(updated);
    }
}
