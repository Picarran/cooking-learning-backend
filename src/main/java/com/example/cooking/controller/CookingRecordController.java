package com.example.cooking.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.example.cooking.common.exception.CookingException;
import com.example.cooking.dto.req.CreateCookingRecordReq;
import com.example.cooking.dto.resp.CookingRecordResp;
import com.example.cooking.dto.resp.Result;
import com.example.cooking.service.CookingRecordService;
import com.example.cooking.utils.UserContext;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/cooking-records")
@RequiredArgsConstructor
public class CookingRecordController {

    private final CookingRecordService cookingRecordService;
    @PostMapping
    public Result<Void> createRecord(@RequestBody CreateCookingRecordReq req) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();

        cookingRecordService.createRecord(req, uid);
        return Result.buildSuccess(null);
    }

    @GetMapping
    public Result<IPage<CookingRecordResp>> listMyRecords(@RequestParam(value = "page", required = false, defaultValue = "1") int page,
                                                          @RequestParam(value = "pageSize", required = false, defaultValue = "10") int pageSize) {
        Long uid = UserContext.getUserId();
        if (uid == null) throw CookingException.UserNotExist();
        IPage<CookingRecordResp> resp = cookingRecordService.listUserRecords(uid, page, pageSize);
        return Result.buildSuccess(resp);
    }
}
