package com.example.cooking.service;

import java.util.Map;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.example.cooking.dao.entity.CookingRecord;
import com.example.cooking.dto.req.CreateCookingRecordReq;
import com.example.cooking.dto.resp.CookingRecordResp;

public interface CookingRecordService {
    void createRecord(CreateCookingRecordReq req, Long userId);

    IPage<CookingRecordResp> listUserRecords(Long userId, int page, int pageSize);
}
