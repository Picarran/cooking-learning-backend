package com.example.cooking.dao.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.TableField;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Data
@TableName("steps")
public class Step {
    @TableId(type = IdType.AUTO)
    private Long id;

    @JsonIgnore
    @TableField("recipe_id")
    private Long recipeId;

    @TableField("step_number")
    private Integer stepNumber;                  // 步骤号

    private String description;                  // 步骤说明

    @TableField(exist = false)
    private TimeRequirement timeRequirement;     // 时间要求（可为null） - JSON only

    @JsonIgnore
    @TableField("time_duration")
    private String timeDuration;

    @JsonIgnore
    @TableField("time_type")
    private String timeType;

    @TableField("target_condition")
    private String targetCondition;              // 目标状态

    @TableField("is_blockable")
    private Boolean isBlockable;                 // 可否停顿做别的

    @TableField("heat_level")
    private String heatLevel;                    // 火候：大火/中火/小火/中大/中小/关火/null

    private String note;                         // 备注

    @TableField(exist = false)
    private String imageUrl; // 不映射到数据库，用于返回给前端
}