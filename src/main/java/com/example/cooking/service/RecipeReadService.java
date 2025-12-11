package com.example.cooking.service;

import com.example.cooking.dao.entity.Recipe;

import java.util.List;

public interface RecipeReadService {
    List<Recipe> listAll(String keyword, String category);

    Recipe findByDishName(String dishName);

    List<Recipe> findByIds(List<Long> ids);
}