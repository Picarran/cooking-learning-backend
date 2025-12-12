package com.example.cooking.common.exception;

import com.example.cooking.dto.resp.Result;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(value = CookingException.class)
    public Result<String> handleAIExternalException(CookingException e) {
        e.printStackTrace();
        return Result.buildFailure(e.getMessage());
    }

    @ExceptionHandler(value = RuntimeException.class)
    public Result<String> handleAIExternalException(RuntimeException e) {
        e.printStackTrace();
        return Result.buildFailure(e.getMessage());
    }
}