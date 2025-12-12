package com.example.cooking.dto.resp;

import lombok.Data;

import java.util.Arrays;
import java.util.List;

@Data
public class ViewsListResp {
    private Long id;          // recipe_view.id
    private Long recipeId;    // recipe.id
    private String dishName;
    private String description;
    private List<String> images;

    /**
     * xml中
     * (
     *   SELECT GROUP_CONCAT(ri.image_url ORDER BY ri.id SEPARATOR ',')
     *   FROM recipe_images ri
     *   WHERE ri.recipe_id = rv.recipe_id
     *  ) AS images
     * MyBatis 自动调用设置
     */
    public void setImages(String imagesStr) {
        if (imagesStr == null || imagesStr.isEmpty()) {
            this.images = List.of();
        } else {
            this.images = Arrays.asList(imagesStr.split(","));
        }
    }
}
