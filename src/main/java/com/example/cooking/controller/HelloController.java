package com.example.cooking.controller;

import com.example.cooking.service.RecipeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class HelloController {
    private final RecipeService recipeService;

    @GetMapping()
    public String sayhello() {
        return "Hello";
    }

    @GetMapping("/repo")
    public String getrepo() {
        return recipeService.listAll(null, null).toString();
    }

}
