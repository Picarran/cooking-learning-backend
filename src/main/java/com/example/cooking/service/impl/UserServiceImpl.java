package com.example.cooking.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.User;
import com.example.cooking.dao.mapper.UserMapper;
import com.example.cooking.dto.resp.UserProfileResp;
import com.example.cooking.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserMapper userMapper;

    @Override
    public User findOrCreateOrUpdateByOpenid(String openid) {
        QueryWrapper<User> qw = new QueryWrapper<>();
        qw.eq("openid", openid);
        User user = userMapper.selectOne(qw);
        if (user == null) {
            user = User.builder()
                    .openid(openid)
                    .points(0L)
                    .avatarUrl(null)
                    .nickname("游客")
                    .build();
            userMapper.insert(user);
            return userMapper.selectById(user.getId());
        }
        return user;
    }

    @Override
    public UserProfileResp getById(Long id) {
        User user = userMapper.selectById(id);
        if (user == null) throw CookingException.UserNotExist();
        return UserProfileResp.builder()
                .id(user.getId())
                .nickname(user.getNickname())
                .avatarUrl(user.getAvatarUrl())
                .points(user.getPoints())
                .cookingCount(user.getCookingCount())
                .cookingTime(user.getCookingTime())
                .build();
    }

    @Override
    public UserProfileResp updateProfile(Long id, String nickname, String avatarUrl) {
        User user = userMapper.selectById(id);
        if (user == null) throw CookingException.UserNotExist();
        if (nickname != null) user.setNickname(nickname);
        if (avatarUrl != null) user.setAvatarUrl(avatarUrl);
        user.setUpdatedAt(new java.util.Date());
        userMapper.updateById(user);

        return UserProfileResp.builder()
                .id(user.getId())
                .nickname(user.getNickname())
                .avatarUrl(user.getAvatarUrl())
                .points(user.getPoints())
                .cookingCount(user.getCookingCount())
                .cookingTime(user.getCookingTime())
                .build();
    }
}
