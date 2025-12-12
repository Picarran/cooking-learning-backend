package com.example.cooking.service.impl;

import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.*;
import com.example.cooking.dao.mapper.OptionalIngredientMapper;
import com.example.cooking.dao.mapper.RecipeImageMapper;
import com.example.cooking.dao.mapper.RecipeMapper;
import com.example.cooking.dao.mapper.RequiredIngredientMapper;
import com.example.cooking.dao.mapper.StepMapper;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.example.cooking.service.RecipeService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RecipeServiceImpl implements RecipeService {
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
    public Recipe findById(Long id) {
        Recipe r = recipeMapper.selectOne(new LambdaQueryWrapper<Recipe>().eq(Recipe::getId, id));
        if (r != null) populateAssociations(r);
        else throw CookingException.RecipeNotExist();
        return r;
    }

    @Override
    public List<Recipe> findByIds(List<Long> ids) {
        List<Recipe> list = recipeMapper.selectBatchIds(ids);
        list.forEach(this::populateAssociations);
        return list;
    }

    @Override
    public Recipe createRecipe(Recipe recipe, Long ownerId) {
        // shallow save recipe fields
        Recipe toSave = new Recipe();
        toSave.setDishName(recipe.getDishName());
        toSave.setDescription(recipe.getDescription());
        toSave.setDifficulty(recipe.getDifficulty());
        toSave.setServings(recipe.getServings());
        toSave.setCategory(recipe.getCategory());
        toSave.setOwnerId(ownerId);

        recipeMapper.insert(toSave);
        Long recipeId = toSave.getId();

        // images
        List<String> images = recipe.getImages();
        if (images != null) {
            for (String img : images) {
                RecipeImage ri = new RecipeImage();
                ri.setRecipeId(recipeId);
                ri.setImageUrl(img);
                recipeImageMapper.insert(ri);
            }
        }

        // ingredients
        Ingredients ing = recipe.getIngredients();
        if (ing != null) {
            if (ing.getRequired() != null) {
                for (IngredientItem it : ing.getRequired()) {
                    RequiredIngredient ri = new RequiredIngredient();
                    ri.setRecipeId(recipeId);
                    ri.setName(it.getName());
                    ri.setAmount(it.getAmount());
                    ri.setNote(it.getNote());
                    requiredIngredientMapper.insert(ri);
                }
            }
            if (ing.getOptional() != null) {
                for (IngredientItem it : ing.getOptional()) {
                    OptionalIngredient oi = new OptionalIngredient();
                    oi.setRecipeId(recipeId);
                    oi.setName(it.getName());
                    oi.setAmount(it.getAmount());
                    oi.setNote(it.getNote());
                    optionalIngredientMapper.insert(oi);
                }
            }
        }

        // steps
        List<Step> steps = recipe.getSteps();
        if (steps != null) {
            for (Step s : steps) {
                Step toStep = new Step();
                toStep.setRecipeId(recipeId);
                toStep.setStepNumber(s.getStepNumber());
                toStep.setDescription(s.getDescription());
                if (s.getTimeRequirement() != null) {
                    toStep.setTimeDuration(s.getTimeRequirement().getDuration());
                    toStep.setTimeType(s.getTimeRequirement().getType());
                }
                toStep.setTargetCondition(s.getTargetCondition());
                toStep.setIsBlockable(s.getIsBlockable());
                toStep.setHeatLevel(s.getHeatLevel());
                toStep.setNote(s.getNote());
                stepMapper.insert(toStep);
            }
        }

        populateAssociations(toSave);
        // return the created recipe with id populated
        return toSave;
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
