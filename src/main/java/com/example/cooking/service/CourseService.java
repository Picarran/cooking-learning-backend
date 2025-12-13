package com.example.cooking.service;

import com.example.cooking.dao.entity.Course;
import com.example.cooking.dto.resp.CourseListResp;

import java.util.List;

public interface CourseService {
    List<CourseListResp> listAll();

    Course findById(Long id);
}
