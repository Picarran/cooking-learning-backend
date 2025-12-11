package com.example.cooking.controller;

import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.dao.mapper.RecipeMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.cooking.service.RecipeReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/recipes")
@RequiredArgsConstructor
public class RecipeController {
    private final RecipeReadService recipeReadService;

    @GetMapping
    public ResponseEntity<List<Recipe>> getAllRecipes(
            @RequestParam(value = "keyword", required = false) String keyword,
            @RequestParam(value = "category", required = false) String category
    ) {
                return ResponseEntity.ok(recipeReadService.listAll(keyword, category));
    }

    @GetMapping("/{dishName}")
    public ResponseEntity<Recipe> getRecipeByName(@PathVariable String dishName) {
        Recipe r = recipeReadService.findByDishName(dishName);
        if (r == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(r);
    }
}
