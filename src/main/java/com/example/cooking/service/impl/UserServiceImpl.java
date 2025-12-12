package com.example.cooking.service.impl;

import com.example.cooking.dao.entity.User;
import com.example.cooking.dao.mapper.UserMapper;
import com.example.cooking.service.UserService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Map;

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
                    .build();
            userMapper.insert(user);
            return userMapper.selectById(user.getId());
        }
        return user;
    }
}
