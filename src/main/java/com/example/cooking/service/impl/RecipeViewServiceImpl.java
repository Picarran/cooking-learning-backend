package com.example.cooking.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.dao.entity.RecipeView;
import com.example.cooking.dao.mapper.RecipeMapper;
import com.example.cooking.dao.mapper.RecipeViewMapper;
import com.example.cooking.dto.resp.ViewsListResp;
import com.example.cooking.service.RecipeService;
import com.example.cooking.service.RecipeViewService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RecipeViewServiceImpl implements RecipeViewService {

    private final RecipeViewMapper recipeViewMapper;
    private final RecipeMapper recipeMapper;
    private final RecipeService recipeService;

    @Override
    public void recordView(Long userId, Long recipeId) {
        if(recipeMapper.selectById(recipeId)==null) throw CookingException.RecipeNotExist();
        RecipeView rv = new RecipeView();
        rv.setRecipeId(recipeId);
        rv.setUserId(userId);
        recipeViewMapper.insert(rv);
    }

    @Override
    public IPage<ViewsListResp> listUserViews(Long userId, int page, int pageSize) {
        return recipeViewMapper.listUserViews(new Page<>(page, pageSize), userId);
    }
}
