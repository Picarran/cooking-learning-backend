package com.example.cooking.dto.resp;

import lombok.Data;

@Data
public class ViewsListResp {
    private Long id;          // recipe_view.id
    private Long recipeId;    // recipe.id
    private String dishName;
    private String description;
    private String images;
}
