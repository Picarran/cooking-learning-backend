package com.example.cooking.dto.req;

import lombok.Data;

@Data
public class UpdateUserReq {
    private String nickname;
    private String avatarUrl;
}
