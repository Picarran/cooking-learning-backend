package com.example.cooking.service.impl;

import com.example.cooking.dao.entity.IngredientItem;
import com.example.cooking.dao.entity.OptionalIngredient;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.dao.entity.RecipeImage;
import com.example.cooking.dao.entity.RequiredIngredient;
import com.example.cooking.dao.entity.Step;
import com.example.cooking.dao.mapper.OptionalIngredientMapper;
import com.example.cooking.dao.mapper.RecipeImageMapper;
import com.example.cooking.dao.mapper.RecipeMapper;
import com.example.cooking.dao.mapper.RequiredIngredientMapper;
import com.example.cooking.dao.mapper.StepMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.cooking.service.RecipeReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RecipeReadServiceImpl implements RecipeReadService {
    private final RecipeMapper recipeMapper;
    private final RecipeImageMapper recipeImageMapper;
    private final RequiredIngredientMapper requiredIngredientMapper;
    private final OptionalIngredientMapper optionalIngredientMapper;
    private final StepMapper stepMapper;

    @Override
    public List<Recipe> listAll(String keyword, String category) {
        LambdaQueryWrapper<Recipe> qw = new LambdaQueryWrapper<>();
        if (keyword != null && !keyword.isBlank()) qw.like(Recipe::getDishName, keyword.trim());
        if (category != null && !category.isBlank()) qw.eq(Recipe::getCategory, category.trim());
        List<Recipe> list = recipeMapper.selectList(qw);
        list.forEach(this::populateAssociations);
        return list;
    }

    @Override
    public Recipe findByDishName(String dishName) {
        Recipe r = recipeMapper.selectOne(new LambdaQueryWrapper<Recipe>().eq(Recipe::getDishName, dishName));
        if (r != null) populateAssociations(r);
        return r;
    }

    @Override
    public List<Recipe> findByIds(List<Long> ids) {
        List<Recipe> list = recipeMapper.selectBatchIds(ids);
        list.forEach(this::populateAssociations);
        return list;
    }

    private void populateAssociations(Recipe r) {
        if (r == null || r.getId() == null) return;
        Long id = r.getId();

        List<RecipeImage> images = recipeImageMapper.selectList(new LambdaQueryWrapper<RecipeImage>().eq(RecipeImage::getRecipeId, id));
        if (images != null) {
            r.setImages(images.stream().map(RecipeImage::getImageUrl).collect(Collectors.toList()));
        }

        List<RequiredIngredient> reqs = requiredIngredientMapper.selectList(new LambdaQueryWrapper<RequiredIngredient>().eq(RequiredIngredient::getRecipeId, id));
        List<OptionalIngredient> opts = optionalIngredientMapper.selectList(new LambdaQueryWrapper<OptionalIngredient>().eq(OptionalIngredient::getRecipeId, id));

        if ((reqs != null && !reqs.isEmpty()) || (opts != null && !opts.isEmpty())) {
            com.example.cooking.dao.entity.Ingredients ing = new com.example.cooking.dao.entity.Ingredients();
            if (reqs != null) {
                List<IngredientItem> required = reqs.stream().map(rg -> {
                    IngredientItem it = new IngredientItem(); it.setName(rg.getName()); it.setAmount(rg.getAmount()); it.setNote(rg.getNote());
                    return it;
                }).collect(Collectors.toList());
                ing.setRequired(required);
            }
            if (opts != null) {
                List<IngredientItem> optional = opts.stream().map(og -> {
                    IngredientItem it = new IngredientItem(); it.setName(og.getName()); it.setAmount(og.getAmount()); it.setNote(og.getNote());
                    return it;
                }).collect(Collectors.toList());
                ing.setOptional(optional);
            }
            r.setIngredients(ing);
        }

        List<Step> steps = stepMapper.selectList(new LambdaQueryWrapper<Step>().eq(Step::getRecipeId, id).orderByAsc(Step::getStepNumber));
        if (steps != null) {
            // convert flat time fields into TimeRequirement for JSON output
            for (Step s : steps) {
                if ((s.getTimeDuration() != null && !s.getTimeDuration().isBlank()) || (s.getTimeType() != null && !s.getTimeType().isBlank())) {
                    com.example.cooking.dao.entity.TimeRequirement tr = new com.example.cooking.dao.entity.TimeRequirement();
                    tr.setDuration(s.getTimeDuration());
                    tr.setType(s.getTimeType());
                    s.setTimeRequirement(tr);
                }
            }
        }
        r.setSteps(steps);
    }
}
