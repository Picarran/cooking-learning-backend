package com.example.cooking.dao.entity;

import lombok.Data;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.TableField;

import java.util.Date;

@Data
@TableName("courses")
public class Course {
    @TableId(type = IdType.AUTO)
    private Long id;

    private String title;

    private String description;

    @TableField("cover_url")
    private String coverUrl;

    @TableField("video_url")
    private String videoUrl;

    @TableField("created_at")
    private Date createdAt;
}
