package com.example.cooking.controller;

import com.example.cooking.dao.entity.Feedback;
import com.example.cooking.dao.mapper.FeedbackMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/feedback")
@RequiredArgsConstructor
public class FeedbackController {

    private final FeedbackMapper feedbackMapper;

    @PostMapping
    public ResponseEntity<Feedback> submit(@RequestBody Feedback feedback) {
        if (feedback.getRating() == null) {
            feedback.setRating(5);
        }
        feedbackMapper.insert(feedback);
        return ResponseEntity.ok(feedback);
    }

    @GetMapping
    public ResponseEntity<List<Feedback>> list() {
        return ResponseEntity.ok(feedbackMapper.selectList(null));
    }
}
