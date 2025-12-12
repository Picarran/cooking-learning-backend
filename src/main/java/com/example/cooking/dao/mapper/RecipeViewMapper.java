package com.example.cooking.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.cooking.dao.entity.RecipeView;
import com.example.cooking.dto.resp.ViewsListResp;
import org.apache.ibatis.annotations.Param;

public interface RecipeViewMapper extends BaseMapper<RecipeView> {
    IPage<ViewsListResp> listUserViews(Page<?> page, @Param("userId") Long userId);
}
