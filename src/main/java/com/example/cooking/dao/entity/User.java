package com.example.cooking.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
@TableName("users")
public class User {
    @TableId(type = IdType.AUTO)
    private Long id;

    private String openid;

    private String nickname;

    @TableField("avatar_url")
    private String avatarUrl;

    private Long points;

    @TableField("cooking_time")
    private Long cookingTime;

    @TableField("cooking_count")
    private Long cookingCount;

    @TableField("created_at")
    private Date createdAt;

    @TableField("updated_at")
    private Date updatedAt;
}
