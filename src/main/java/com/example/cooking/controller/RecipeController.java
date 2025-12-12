package com.example.cooking.controller;

import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.service.RecipeReadService;
import lombok.RequiredArgsConstructor;
import com.example.cooking.dto.Result;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/recipes")
@RequiredArgsConstructor
public class RecipeController {
    private final RecipeReadService recipeReadService;

    @GetMapping
    public Result<List<Recipe>> getAllRecipes(
            @RequestParam(value = "keyword", required = false) String keyword,
            @RequestParam(value = "category", required = false) String category
    ) {
                return Result.buildSuccess(recipeReadService.listAll(keyword, category));
    }

    @GetMapping("/{dishName}")
    public Result<Recipe> getRecipeByName(@PathVariable String dishName) {
        Recipe r = recipeReadService.findByDishName(dishName);
        if (r == null) return new Result<>(404, "not found", null);
        return Result.buildSuccess(r);
    }
}
