package com.example.cooking.common.exception;

public class CookingException extends RuntimeException {

    public CookingException(String message) {
        super(message);
    }

    public static CookingException noToken() {
        return new CookingException("未携带登录token!");
    }

    public static CookingException tokenValidateFail() {
        return new CookingException("非法token或token过期!");
    }

    public static CookingException UserNotExist() {
        return new CookingException("用户不存在!");
    }

}