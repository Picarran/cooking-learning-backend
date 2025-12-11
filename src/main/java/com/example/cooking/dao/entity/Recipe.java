package com.example.cooking.dao.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import java.util.List;

@Data
@TableName("recipes")
public class Recipe {
    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField("dish_name")
    private String dishName;           // 菜名

    @TableField(exist = false)
    private List<String> images;       // 图片列表 (not a direct column)

    private String description;        // 描述

    private Integer difficulty;        // 难度 1-5

    private String servings;           // 份量

    private String category;           // 分类（enum）

    @TableField(exist = false)
    private Ingredients ingredients;   // 配料 (stored in separate tables)

    @TableField(exist = false)
    private List<Step> steps;          // 步骤 (stored in steps table)
}
