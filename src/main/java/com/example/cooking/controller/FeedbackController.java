package com.example.cooking.controller;

import com.example.cooking.dao.entity.Feedback;
import com.example.cooking.dao.mapper.FeedbackMapper;
import lombok.RequiredArgsConstructor;
import com.example.cooking.dto.Result;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/feedback")
@RequiredArgsConstructor
public class FeedbackController {

    private final FeedbackMapper feedbackMapper;

    @PostMapping
    public Result<Feedback> submit(@RequestBody Feedback feedback) {
        if (feedback.getRating() == null) {
            feedback.setRating(5);
        }
        feedbackMapper.insert(feedback);
        return Result.buildSuccess(feedback);
    }

    @GetMapping
    public Result<List<Feedback>> list() {
        return Result.buildSuccess(feedbackMapper.selectList(null));
    }
}
