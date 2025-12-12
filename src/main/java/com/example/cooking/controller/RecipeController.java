package com.example.cooking.controller;

import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.service.RecipeService;
import com.example.cooking.dao.mapper.RecipeViewMapper;
import com.example.cooking.dao.entity.RecipeView;
import com.example.cooking.utils.UserContext;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.core.metadata.IPage;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import com.example.cooking.dto.resp.Result;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/recipes")
@RequiredArgsConstructor
public class RecipeController {
    private final RecipeService recipeService;
    private final RecipeViewMapper recipeViewMapper;

    @GetMapping
    public Result<List<Recipe>> getAllRecipes(
            @RequestParam(value = "keyword", required = false) String keyword,
            @RequestParam(value = "category", required = false) String category
    ) {
        return Result.buildSuccess(recipeService.listAll(keyword, category));
    }

    @PostMapping
    public Result<Recipe> createRecipe(@RequestBody Recipe recipe) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();
        Recipe created = recipeService.createRecipe(recipe, uid);
        return Result.buildSuccess(created);
    }

    @GetMapping("/{id}")
    public Result<Recipe> getRecipeById(@PathVariable Long id) {
        return Result.buildSuccess(recipeService.findById(id));
    }

}
