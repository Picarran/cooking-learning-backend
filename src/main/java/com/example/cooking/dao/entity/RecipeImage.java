package com.example.cooking.dao.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@Data
@TableName("recipe_images")
public class RecipeImage {
    @TableId(type = IdType.AUTO)
    private Long id;

    private Long recipeId;

    private String imageUrl;
}
