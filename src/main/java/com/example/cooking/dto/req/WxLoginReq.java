package com.example.cooking.dto.req;

import lombok.Data;

@Data
public class WxLoginReq {
    private String code;
    private String appId;
    private String secret;
}