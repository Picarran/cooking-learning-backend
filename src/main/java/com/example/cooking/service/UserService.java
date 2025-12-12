package com.example.cooking.service;

import com.example.cooking.dao.entity.User;
import com.example.cooking.dto.resp.UserProfileResp;

import java.util.Map;

public interface UserService {
    /**
     * 根据 openid 查找用户；若不存在则创建；若存在则根据 userInfo 更新昵称/头像并返回
     */
    User findOrCreateOrUpdateByOpenid(String openid);

    /**
     * 根据 id 查询用户
     */
    UserProfileResp getById(Long id);

    /**
     * 更新用户昵称和头像，返回更新后的用户
     */
    UserProfileResp updateProfile(Long id, String nickname, String avatarUrl);
}
