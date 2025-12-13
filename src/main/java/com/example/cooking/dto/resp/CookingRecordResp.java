package com.example.cooking.dto.resp;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

@Data
public class CookingRecordResp {
    private Long id;          // recipe_view.id
    private Long recipeId;    // recipe.id
    private String dishName;
    private String description;
    private List<String> images;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime startedAt;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime finishedAt;

    private Integer rating; // 1-5
    private String notes;

    /**
     * xml中MyBatis 自动调用设置
     */
    public void setImages(String imagesStr) {
        if (imagesStr == null || imagesStr.isEmpty()) {
            this.images = List.of();
        } else {
            this.images = Arrays.asList(imagesStr.split(","));
        }
    }
}
