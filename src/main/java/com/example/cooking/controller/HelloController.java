package com.example.cooking.controller;

import com.example.cooking.service.RecipeReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class HelloController {
    private final RecipeReadService recipeReadService;

    @GetMapping()
    public String sayhello() {
        return "Hello";
    }

    @GetMapping("/repo")
    public String getrepo() {
        return recipeReadService.listAll(null, null).toString();
    }

}
