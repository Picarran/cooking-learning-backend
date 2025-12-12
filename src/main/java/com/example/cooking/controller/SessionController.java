package com.example.cooking.controller;

import com.example.cooking.dao.entity.CookingRuntime;
import com.example.cooking.dao.entity.IngredientItem;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.service.CookingService;
import com.example.cooking.service.RecipeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import com.example.cooking.dto.resp.Result;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class SessionController {

    private final CookingService cookingService;
    private final RecipeService recipeService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/sessions")
    public Result<Map<String, String>> createSession(@RequestBody CreateSessionRequest req) {
        String sid = UUID.randomUUID().toString();
        ArrayNode arr = objectMapper.createArrayNode();
        req.getDishes().forEach(arr::add);
        boolean ok = cookingService.createSession(sid, arr);
        if (!ok) {
            return Result.buildFailure(Map.of("error", "创建会话失败，菜名不存在"));
        }
        return Result.buildSuccess(Map.of("sessionId", sid));
    }

    @PostMapping("/sessions/{sid}/next")
    public Result<Map<String, Object>> next(@PathVariable String sid) {
        boolean ok = cookingService.pollNextStepAndConsume(sid);
        return Result.buildSuccess(Map.of("success", ok));
    }

    @PostMapping("/sessions/{sid}/blockable")
    public Result<Map<String, Object>> startBlockable(@PathVariable String sid) {
        boolean ok = cookingService.startBlockabled(sid);
        return Result.buildSuccess(Map.of("success", ok));
    }

    @GetMapping("/sessions/{sid}")
    public Result<?> sessionState(@PathVariable String sid) {
        Optional<CookingRuntime> runtimeOpt = cookingService.getRuntime(sid);
        if (runtimeOpt.isPresent()) {
            return Result.buildSuccess(runtimeOpt.get());
        }
        return new Result<>(404, "not found", null);
    }

    @GetMapping("/shopping-list")
    public Result<Map<String, Object>> shoppingList(@RequestParam List<Long> recipeIds) {
        List<Recipe> recipes = recipeService.findByIds(recipeIds);
        List<IngredientItem> all = new ArrayList<>();
        for (Recipe r : recipes) {
            if (r.getIngredients() != null && r.getIngredients().getRequired() != null) {
                all.addAll(r.getIngredients().getRequired());
            }
        }
        // 简单合并同名食材
        Map<String, List<String>> merged = all.stream()
                .collect(Collectors.groupingBy(IngredientItem::getName,
                        Collectors.mapping(IngredientItem::getAmount, Collectors.toList())));

        List<Map<String, String>> items = merged.entrySet().stream().map(e -> {
            String amount = String.join(" / ", e.getValue());
            return Map.of("name", e.getKey(), "amount", amount);
        }).toList();

        return Result.buildSuccess(Map.of(
            "count", items.size(),
            "items", items
        ));
    }

    @Data
    public static class CreateSessionRequest {
        // 支持两种格式：直接传菜名数组，或 { dishes: ["红烧肉"] }
        private List<String> dishes = new ArrayList<>();
    }
}
