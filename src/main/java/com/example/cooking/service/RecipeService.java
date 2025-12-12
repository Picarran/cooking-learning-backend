package com.example.cooking.service;

import com.example.cooking.dao.entity.Recipe;

import java.util.List;

public interface RecipeService {
    List<Recipe> listAll(String keyword, String category);

    Recipe findByDishName(String dishName);

    Recipe findById(Long id);

    List<Recipe> findByIds(List<Long> ids);

    Recipe createRecipe(Recipe recipe, Long ownerId);
}