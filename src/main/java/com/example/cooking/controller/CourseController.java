package com.example.cooking.controller;

import com.example.cooking.dao.entity.Course;
import com.example.cooking.dto.resp.CourseListResp;
import com.example.cooking.dto.resp.Result;
import com.example.cooking.service.CourseService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/courses")
@RequiredArgsConstructor
public class CourseController {

    private final CourseService courseService;

    @GetMapping
    public Result<List<CourseListResp>> listCourses() {
        return Result.buildSuccess(courseService.listAll());
    }

    @GetMapping("/{courseId}")
    public Result<Course> getCourse(@PathVariable Long courseId) {
        Course c = courseService.findById(courseId);
        if (c == null) return Result.buildFailure(null);
        return Result.buildSuccess(c);
    }
}
