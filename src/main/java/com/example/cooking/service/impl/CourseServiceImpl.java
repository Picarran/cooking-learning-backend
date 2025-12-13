package com.example.cooking.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.Course;
import com.example.cooking.dao.mapper.CourseMapper;
import com.example.cooking.dto.resp.CourseListResp;
import com.example.cooking.service.CourseService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
@RequiredArgsConstructor
public class CourseServiceImpl implements CourseService {

    private final CourseMapper courseMapper;

    @Override
    public List<CourseListResp> listAll() {
        return courseMapper.selectList(new LambdaQueryWrapper<>())
                .stream()
                .map(course -> BeanUtil.toBean(course, CourseListResp.class))
                .toList();
    }

    @Override
    public Course findById(Long id) {
        Course course = courseMapper.selectById(id);
        if(course==null) throw CookingException.CourseNotExist();
        return course;
    }
}
