版本：v1
鉴权

1) 微信小程序登录
- URL: POST /api/auth/wxlogin
- 描述: 小程序端把 wx.login() 获得的 code 发送到后端，后端向微信换取 openid，并在本地创建/更新用户，返回 JWT 与用户信息。
- 请求参数 Params
参数
类型
说明
code
String
微信小程序 wx.login 返回的 code（必填）
appId
String
小程序 AppId（可选，若服务端已配置可不传）
secret
String
小程序 AppSecret（可选，若服务端已配置可不传）

- 成功响应:
{
  "code": 200,
  "msg": "success",
  "data": {
    "id": 4,
    "nickname": "游客",
    "avatarUrl": "https://...",
    "points": 0,
    "token": "93fe9ba29a014f30b284a1ca2a5c1c98"
  }
}



2) 通用鉴权
- 描述: 受保护接口需在请求头带 Authorization: Bearer <token>。后端验证 token 签名与过期时间，并把 userId 注入请求上下文。
Header
说明
Authorization
Bearer <token>，token 来自 /api/auth/wxlogin 的 data.token


---

用户（profile）


GET /api/users/me
- URL: GET /api/users/me
- 描述: 获取当前登录用户信息
- 需鉴权
- 成功响应:
{
  "code":200,
  "msg":"success",
  "data":{
    "id":101,
    "nickname":"张三",
    "avatarUrl":"https://...",
    "points":120
  }
}
- point表示用户积分，前端可简单根据此给用户一个等级：
  - 学徒：(0, 199)
熟手：(200, 499)
高手：(500, 999)
厨神：(1000, 999999)
PUT /api/users/me
- URL: PUT /api/users/me
- 描述: 更新用户信息（昵称、头像）
- 需鉴权
- 请求参数 Params
参数
类型
说明
nickname
String
昵称（可选）
avatarUrl
String
头像 URL（可选）

- 成功响应示例:
{
  "code":200,
  "msg":"success",
  "data":{
    "id":101,
    "nickname":"张三",
    "avatarUrl":"https://...",
    "points":120
  }
}


---

菜谱（recipes）


GET /api/recipes
- URL: GET /api/recipes
- 描述: 菜谱列表查询（每项会包含 images, ingredients, steps_summary）
- 参数 Params
参数
类型
说明
keyword
String
搜索关键词（可选）
category
String
分类（可选）
- 成功响应示例:
{
  "code": 200,
  "msg": "success",
  "data": [
    {
      "id": 1,
      "dishName": "简易红烧肉",
      "images": [
        "dishes/meat_dish/红烧肉/000.jpg",
        "dishes/meat_dish/红烧肉/001.jpg"
      ],
      "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      "difficulty": 3,
      "servings": "2-3人份",
      "category": "荤菜",
      "ingredients": {
        "required": [
          { "name": "猪五花肉", "amount": "约3-4斤", "note": null },
          { "name": "冰糖", "amount": "20g", "note": null },
          { "name": "生抽", "amount": "2勺", "note": null },
          { "name": "老抽", "amount": "1勺", "note": "调色用" },
          { "name": "姜片", "amount": "3片", "note": null },
          { "name": "料酒", "amount": "1勺", "note": null }
        ],
        "optional": [
          { "name": "鹌鹑蛋或鸡蛋", "amount": "0-2个", "note": null },
          { "name": "豆皮", "amount": "适量", "note": null }
        ]
      },
      "steps": [
        {
          "id": 1,
          "stepNumber": 1,
          "description": "五花肉切块，冷水下锅焯水后捞出备用。",
          "timeRequirement": { "duration": "5分钟", "type": "approx" },
          "imageUrl": "dishes/meat_dish/红烧肉/step_01.jpg"
        },
        ...
      ]
    }
    ...
  ]
}

GET /api/recipes/{id}
- URL: GET /api/recipes/{id}
- 描述: 获取单个菜谱详情（包含 images, required_ingredients, optional_ingredients, steps）
- 成功响应示例:
{
  "code": 200,
  "msg": "success",
  "data": 
  {
      "id": 1,
      "dishName": "简易红烧肉",
      "owner_id": null,   // null 表示系统导入
      "images": [
        "dishes/meat_dish/红烧肉/000.jpg",
        "dishes/meat_dish/红烧肉/001.jpg"
      ],
      "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      "difficulty": 3,
      "servings": "2-3人份",
      "category": "荤菜",
      "ingredients": {
        "required": [
          { "name": "猪五花肉", "amount": "约3-4斤", "note": null },
          { "name": "冰糖", "amount": "20g", "note": null },
          { "name": "生抽", "amount": "2勺", "note": null },
          { "name": "老抽", "amount": "1勺", "note": "调色用" },
          { "name": "姜片", "amount": "3片", "note": null },
          { "name": "料酒", "amount": "1勺", "note": null }
        ],
        "optional": [
          { "name": "鹌鹑蛋或鸡蛋", "amount": "0-2个", "note": null },
          { "name": "豆皮", "amount": "适量", "note": null }
        ]
      },
      "steps": [
        {
          "id": 1,
          "stepNumber": 1,
          "description": "五花肉切块，冷水下锅焯水后捞出备用。",
          "timeRequirement": { "duration": "5分钟", "type": "approx" },
          "imageUrl": "dishes/meat_dish/红烧肉/step_01.jpg"
        },
        ...
      ]
    }
  }
}

POST /api/recipes
- URL: POST /api/recipes
- 描述: 用户上传菜谱（需鉴权），后端会保存并将 owner_id 设为当前用户
- 请求参数 Body 示例（JSON）
{
      "dishName": "简易红烧肉",
      "images": [
        "dishes/meat_dish/红烧肉/000.jpg",
        "dishes/meat_dish/红烧肉/001.jpg"
      ],
      "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      "difficulty": 3,
      "servings": "2-3人份",
      "category": "荤菜",
      "ingredients": {
        "required": [
          { "name": "猪五花肉", "amount": "约3-4斤", "note": null },
          { "name": "冰糖", "amount": "20g", "note": null },
          { "name": "生抽", "amount": "2勺", "note": null },
          { "name": "老抽", "amount": "1勺", "note": "调色用" },
          { "name": "姜片", "amount": "3片", "note": null },
          { "name": "料酒", "amount": "1勺", "note": null }
        ],
        "optional": [
          { "name": "鹌鹑蛋或鸡蛋", "amount": "0-2个", "note": null },
          { "name": "豆皮", "amount": "适量", "note": null }
        ]
      },
      "steps": [
        {
          "id": 1,
          "stepNumber": 1,
          "description": "五花肉切块，冷水下锅焯水后捞出备用。",
          "timeRequirement": { "duration": "5分钟", "type": "approx" },
          "imageUrl": "dishes/meat_dish/红烧肉/step_01.jpg"
        },
        ...
      ]
    }
  }

- 成功响应:
{
  "code": 200,
  "msg": "success",
  "data": 
  {
      "id": 1,
      "dishName": "简易红烧肉",
      "owner_id": 1,   // null 表示系统导入
      "images": [
        "dishes/meat_dish/红烧肉/000.jpg",
        "dishes/meat_dish/红烧肉/001.jpg"
      ],
      "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      "difficulty": 3,
      "servings": "2-3人份",
      "category": "荤菜",
      "ingredients": {
        "required": [
          { "name": "猪五花肉", "amount": "约3-4斤", "note": null },
          { "name": "冰糖", "amount": "20g", "note": null },
          { "name": "生抽", "amount": "2勺", "note": null },
          { "name": "老抽", "amount": "1勺", "note": "调色用" },
          { "name": "姜片", "amount": "3片", "note": null },
          { "name": "料酒", "amount": "1勺", "note": null }
        ],
        "optional": [
          { "name": "鹌鹑蛋或鸡蛋", "amount": "0-2个", "note": null },
          { "name": "豆皮", "amount": "适量", "note": null }
        ]
      },
      "steps": [
        {
          "id": 1,
          "stepNumber": 1,
          "description": "五花肉切块，冷水下锅焯水后捞出备用。",
          "timeRequirement": { "duration": "5分钟", "type": "approx" },
          "imageUrl": "dishes/meat_dish/红烧肉/step_01.jpg"
        },
        ...
      ]
    }
  }
}


---

浏览记录（views）

POST /api/view/{recipeId}
- URL: POST /api/view/{recipeId}
- 描述: 记录一次浏览（前端在详情打开时调用）。
- 请求参数: 无
- 成功响应: 
{
  "code":200,
  "msg":"success",
  "data": null
}
GET /api/views
- 描述: 获取用户最近浏览记录（分页）
- 需鉴权
- 请求参数: 
参数
类型
说明
page
Int
页码（默认1）
pageSize
Int
每页大小（默认10）
- 成功响应: 
{
  "code": 200,
  "msg": "success",
  "data": {
    "records": [
      {
        "id": 1,
        "recipeId": 1,
        "dishName": "简易红烧肉",
        "images": [
          "dishes/meat_dish/红烧肉/000.jpg",
          "dishes/meat_dish/红烧肉/001.jpg"
        ],
        "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      }
      ...
    ],
    "total": 3,
    "size": 2,
    "current": 1,
    "pages": 2
  }
}



---

做菜记录（cooking-records）

POST /api/cooking-records
- URL: POST /api/users/me/cooking-records
- 描述: 提交一次做菜记录（需鉴权），记录会写入 cooking_records，并触发积分更新（user_points）
- 请求参数 Params
参数
类型
说明
recipeId
Int
菜谱 ID（必填）
startedAt
DATETIME
开始时间（可选）
finishedAt
DATETIME
结束时间（可选）
durationSeconds
Int
持续秒数（可选）
rating
Int
用户评分 1-5（可选）
notes
String
备注（可选）

- 成功响应:
{
  "code":200,
  "msg":"success",
  "data": null
}

- 后端会更新用户的积分point。

GET /api/users/me/cooking-records
- URL: GET /api/cooking-records
- 描述: 分页查询当前用户的做菜记录（需鉴权）
- 请求参数: 
参数
类型
说明
page
Int
页码（默认1）
pageSize
Int
每页大小（默认10）
- 成功响应: 
{
  "code": 200,
  "msg": "success",
  "data": {
    "records": [
      {
        "id": 1,
        "recipeId": 1,
        "dishName": "简易红烧肉",
        "images": [
          "dishes/meat_dish/红烧肉/000.jpg",
          "dishes/meat_dish/红烧肉/001.jpg"
        ],
        "description": "这份红烧肉教程是一道新手不败的菜谱。配着米饭好吃的停不下来，香糯无敌棒，色泽诱人、肥而不腻。建议搭配米饭食用。",
      }
      ...
    ],
    "total": 3,
    "size": 2,
    "current": 1,
    "pages": 2
  }
}



---

课程（courses）

GET /api/courses
- URL: GET /api/courses
- 成功响应: 
- 描述: 列出课程
{
  "code": 200,
  "msg": "success",
  "data": [
    {
      "id": 1,
      "title": "零基础入门烹饪",
      "description": "从零开始学做菜，让你轻松掌握厨房技巧。",
      "coverUrl": "https://example.com/images/course1.jpg"
    },
    {
      "id": 2,
      "title": "红烧肉大师课",
      "description": "带你掌握经典红烧肉制作技巧。",
      "coverUrl": "https://example.com/images/course2.jpg"
    }
  ]
}
GET /api/courses/{courseId}
- 描述: 课程详情
- 成功响应: 
{
  "code": 200,
  "msg": "success",
  "data": {
      "id": 1,
      "title": "零基础入门烹饪",
      "description": "从零开始学做菜，让你轻松掌握厨房技巧。",
      "coverUrl": "https://example.com/images/course1.jpg",
      "videoUrl": "https://example.com/videos/course1.mp4",
      "createdAt": "2025-01-15 10:20:30"
    }
}
