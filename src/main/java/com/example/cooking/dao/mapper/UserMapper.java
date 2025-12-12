package com.example.cooking.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.cooking.dao.entity.User;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper extends BaseMapper<User> {

}
