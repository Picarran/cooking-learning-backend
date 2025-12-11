package com.example.cooking.dao.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@Data
@TableName("optional_ingredients")
public class OptionalIngredient {
    @TableId(type = IdType.AUTO)
    private Long id;

    private Long recipeId;

    private String name;

    private String amount;

    private String note;
}
