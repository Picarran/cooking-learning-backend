package com.example.cooking.utils;

import ch.qos.logback.core.util.StringUtil;
import cn.hutool.core.lang.UUID;
import com.example.cooking.common.exception.CookingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

@Component
public class UploadUtil {
    @Value("${ecs.upload-image-uri}")
    private String IMAGE_URI;
    @Value("${ecs.upload-image-path}")
    private String BASE_DIR;
    public String uploadImage(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            throw new RuntimeException("文件为空");
        }

        // 校验类型
        String contentType = file.getContentType();
        if (contentType == null || !contentType.startsWith("image/")) {
            throw new RuntimeException("只允许上传图片");
        }

        // 尾缀
        String ext = StringUtils.getFilenameExtension(file.getOriginalFilename());
        if (StringUtil.isNullOrEmpty(ext)) ext = "png";

        // 生成安全文件名
        String filename = UUID.randomUUID().toString().replace("-", "") + "." + ext;

        // 按日期分目录
        String datePath = LocalDate.now().toString().replace("-", "/");
        Path dir = Paths.get(BASE_DIR, datePath);
        try {
            Files.createDirectories(dir);

            Path target = dir.resolve(filename);
            file.transferTo(target);
        } catch (IOException e) {
            e.printStackTrace();
            throw CookingException.uploadImageFail();
        }
        return IMAGE_URI + datePath + "/" + filename;
    }
}
