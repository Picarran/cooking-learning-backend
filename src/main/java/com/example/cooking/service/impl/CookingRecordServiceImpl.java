package com.example.cooking.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dao.entity.CookingRecord;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.dao.entity.User;
import com.example.cooking.dao.mapper.CookingRecordMapper;
import com.example.cooking.dao.mapper.RecipeMapper;
import com.example.cooking.dao.mapper.UserMapper;
import com.example.cooking.dto.req.CreateCookingRecordReq;
import com.example.cooking.dto.resp.CookingRecordResp;
import com.example.cooking.service.CookingRecordService;
import com.example.cooking.service.RecipeService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CookingRecordServiceImpl implements CookingRecordService {

    private final CookingRecordMapper cookingRecordMapper;
    private final RecipeMapper recipeMapper;
    private final RecipeService recipeService;
    private final UserMapper userMapper;
    @Transactional
    @Override
    public void createRecord(CreateCookingRecordReq req, Long userId) {
        Recipe recipe = recipeMapper.selectById(req.getRecipeId());
        if(recipe==null) throw CookingException.RecipeNotExist();
        User user = userMapper.selectById(userId);
        if(user==null) throw CookingException.UserNotExist();

        CookingRecord cr = CookingRecord.builder()
                .recipeId(req.getRecipeId())
                .notes(req.getNotes())
                .finishedAt(req.getFinishedAt())
                .startedAt(req.getStartedAt())
                .rating(req.getRating())
                .userId(userId)
                .build();

        cookingRecordMapper.insert(cr);
        user.setPoints(user.getPoints() + recipe.getDifficulty() * 10);
        userMapper.updateById(user);
    }

    @Override
    public IPage<CookingRecordResp> listUserRecords(Long userId, int page, int pageSize) {
        return cookingRecordMapper.listUserRecord(new Page<>(page, pageSize), userId);
    }
}
