package com.example.cooking.service;

import com.example.cooking.dao.entity.User;

import java.util.Map;

public interface UserService {
    /**
     * 根据 openid 查找用户；若不存在则创建；若存在则根据 userInfo 更新昵称/头像并返回
     */
    User findOrCreateOrUpdateByOpenid(String openid);
}
