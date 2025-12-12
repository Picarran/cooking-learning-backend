package com.example.cooking.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.RecipeView;
import com.example.cooking.dao.mapper.RecipeViewMapper;
import com.example.cooking.dto.resp.Result;
import com.example.cooking.dto.resp.ViewsListResp;
import com.example.cooking.service.RecipeService;
import com.example.cooking.utils.UserContext;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/views")
@RequiredArgsConstructor
public class ViewController {

    private final RecipeService recipeService;
    private final com.example.cooking.service.RecipeViewService recipeViewService;
    @PostMapping("/view/{recipeId}")
    public Result<?> recordView(@PathVariable Long recipeId) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();
        recipeViewService.recordView(uid, recipeId);
        return Result.buildSuccess(null);
    }

    @GetMapping("/views")
    public Result<IPage<ViewsListResp>> listMyViews(
            @RequestParam(value = "page", required = false, defaultValue = "1") int page,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") int pageSize
    ) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();

        IPage<ViewsListResp> resp = recipeViewService.listUserViews(uid, page, pageSize);
        return Result.buildSuccess(resp);
    }
}
