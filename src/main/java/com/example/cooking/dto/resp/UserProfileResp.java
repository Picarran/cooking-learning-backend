package com.example.cooking.dto.resp;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserProfileResp {
    private Long id;
    private String nickname;
    private String avatarUrl;
    private Long points;
    private Long cookingTime;
    private Long cookingCount;
}
