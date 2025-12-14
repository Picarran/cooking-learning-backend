package com.example.cooking.config;

import com.example.cooking.dao.entity.*;
import com.example.cooking.dao.mapper.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "app.data-initializer", name = "enabled", havingValue = "true")
public class DataInitializer implements CommandLineRunner {

    private final RecipeMapper recipeMapper;
    private final RequiredIngredientMapper requiredIngredientMapper;
    private final OptionalIngredientMapper optionalIngredientMapper;
    private final StepMapper stepMapper;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run(String... args) throws Exception {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("data.json");
        if (is == null) {
            System.out.println("DataInitializer: data.json not found in classpath, skipping");
            return;
        }

        List<Recipe> recipes = objectMapper.readValue(is, new TypeReference<List<Recipe>>() {});

        if (recipes == null || recipes.isEmpty()) {
            System.out.println("DataInitializer: no recipes found in data.json");
            return;
        }

        for (Recipe r : recipes) {
            // prepare a shallow copy to save recipe fields only
            Recipe toSave = new Recipe();
            toSave.setDishName(r.getDishName());
            toSave.setDescription(r.getDescription());
            toSave.setDifficulty(r.getDifficulty());
            toSave.setServings(r.getServings());
            toSave.setCategory(r.getCategory());
            toSave.setImages(r.getImages());

            recipeMapper.insert(toSave);
            Long recipeId = toSave.getId();

            // ingredients
            if (r.getIngredients() != null) {
                if (r.getIngredients().getRequired() != null) {
                    for (IngredientItem it : r.getIngredients().getRequired()) {
                        RequiredIngredient ri = new RequiredIngredient();
                        ri.setRecipeId(recipeId);
                        ri.setName(it.getName());
                        ri.setAmount(it.getAmount());
                        ri.setNote(it.getNote());
                        requiredIngredientMapper.insert(ri);
                    }
                }
                if (r.getIngredients().getOptional() != null) {
                    for (IngredientItem it : r.getIngredients().getOptional()) {
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
            if (r.getSteps() != null) {
                for (Step s : r.getSteps()) {
                    Step toStep = new Step();
                    toStep.setRecipeId(recipeId);
                    toStep.setStepNumber(s.getStepNumber());
                    toStep.setRecipeId(recipeId);
                    toStep.setDescription(s.getDescription());
                    toStep.setImageUrl(s.getImageUrl());
                    if (s.getTimeRequirement() != null) {
                        toStep.setTimeRequirement(s.getTimeRequirement());
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
        }

        System.out.println("DataInitializer: inserted " + recipes.size() + " recipes");
    }
}
 