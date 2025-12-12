package com.example.cooking.dto.resp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result <T> {
    private int code;
    private String msg;
    private T data;

    public static <T> Result<T> buildSuccess(T data) {
        return new Result<T>(200, "success", data);
    }

    public static <T> Result<T> buildFailure(T data) {
        return new Result<T>(400, "failure", data);
    }
}
