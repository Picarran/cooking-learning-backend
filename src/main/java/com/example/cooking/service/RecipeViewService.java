package com.example.cooking.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.cooking.dto.resp.ViewsListResp;

import java.util.Map;

public interface RecipeViewService {
    void recordView(Long userId, Long recipeId);

    IPage<ViewsListResp> listUserViews(Long userId, int page, int pageSize);
}
