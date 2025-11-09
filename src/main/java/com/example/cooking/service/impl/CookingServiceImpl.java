package com.example.cooking.service.impl;

import com.example.cooking.common.web.WsHandler;
import com.example.cooking.dao.entity.Recipe;
import com.example.cooking.dao.entity.Step;
import com.example.cooking.dao.repository.RecipeRepository;
import com.example.cooking.dto.CookingRuntime;
import com.example.cooking.service.CookingService;
import com.example.cooking.utils.TimeParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Service
@RequiredArgsConstructor
public class CookingServiceImpl implements CookingService {

    private final Map<String, CookingRuntime> cookingMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);

    private final RecipeRepository recipeRepository;

    private static final ObjectMapper M = new ObjectMapper();

    // create session from dishNames JSON array node (from WsHandler)
    public Boolean createSessionWithDishNames(String sid, com.fasterxml.jackson.databind.JsonNode dishNamesNode) {
        List<Recipe> recipes = new ArrayList<>();
        // 从数据库找并加入recipes
        for (com.fasterxml.jackson.databind.JsonNode n : dishNamesNode) {
            String name = n.asText();
            Recipe r = recipeRepository.findByName(name);
            if(r!=null){
                recipes.add(r);
            }else{
                System.err.println("no such dish: " + name);
                return false;
            }
        }

        Map<Integer, Integer> stepMap = new HashMap<>();
        for(int i=0;i<recipes.size();i++){
            stepMap.put(i,-1);
        }
        CookingRuntime cookingRuntime = CookingRuntime.builder()
                .sid(sid)
                .recipes(recipes)
                .stepMap(stepMap)
                .currentRecipeIndex(0)
                .taskMap(new HashMap<>())
                .build();

        cookingMap.put(sid, cookingRuntime);

        return true;
    }


    // optional: bind by userId
//    public void bindUserWs(String userId, String wsSessionId) {
//        // 简单实现：把 userId 存到所有匹配 session 上（或维护 user->session 映射）
//        // 此处省略复杂逻辑
//    }

    // unbind wsSessionId（连接断开）
    public void unbindWsSession(String sid) {
        cookingMap.remove(sid);
    }

    // 客户端主动拉取下一步（并消费）
    public String pollNextStepAndConsume(String sid) {
        if(!currentRecipeExist(sid)){
            if(!checkTasksExist(sid)) {
                // 没有下一步且没有菜在等待
                WsHandler.sendToWsSession(sid, "all dishes done");
            }
            return null;
        }


        // 处理：目前指向blockable步但还没开始
        if(currentStepIsBlockableButNotStart(sid)) {
            System.out.println("currentStepIsBlockableButNotStart, need call block start");
            WsHandler.sendToWsSession(sid, "currentStepIsBlockableButNotStart, need call block start");
            return null;
        }

        if(gotoNextStepifPresent(sid)){
            CookingRuntime cookingRuntime = cookingMap.get(sid);
            int curRecipeIdx = cookingRuntime.getCurrentRecipeIndex();
            List<Recipe> recipes = cookingRuntime.getRecipes();
            Recipe curRecipe = recipes.get(curRecipeIdx);
            String dishName = curRecipe.getDishName();

            int curStepIdx = cookingRuntime.getStepMap().get(curRecipeIdx);
            Step step = curRecipe.getSteps().get(curStepIdx);

            if(step.getIsBlockable()){
                System.out.println("current step is blockable");
            }
            return toJsonStep(dishName, step);
        } else if(!checkTasksExist(sid)) {
            // 没有下一步且没有菜在等待
            WsHandler.sendToWsSession(sid, "all dishes done");
        }
        return null;

    }

    private Boolean checkTasksExist(String sid){
        CookingRuntime cookingRuntime = cookingMap.get(sid);
        for(Map.Entry<String, ScheduledFuture<?>> e : cookingRuntime.getTaskMap().entrySet()){
            String key = e.getKey();
            ScheduledFuture<?> task = e.getValue();
//                Long delay = task.getDelay(TimeUnit.MINUTES);
            Long delay = task.getDelay(TimeUnit.SECONDS);
            WsHandler.sendToWsSession(sid, key + "wait... left: " + delay + "seconds");
//                WsHandler.sendToWsSession(sid, key + "wait... left: " + delay + "minutes");
            return true;
        }
        return false;
    }
    public Boolean startBlockabled(String sid){
        if(!currentRecipeExist(sid)){
            return false;  // 理论上不能到这里
        }
        CookingRuntime cookingRuntime = cookingMap.get(sid);

        List<Recipe> recipes = cookingRuntime.getRecipes();
        int curRecipeIdx = cookingRuntime.getCurrentRecipeIndex();
        Recipe curRecipe = recipes.get(curRecipeIdx);

        int curStepIdx = cookingRuntime.getStepMap().get(curRecipeIdx);
        Step step = curRecipe.getSteps().get(curStepIdx);

        // 如果blockable，设定定时任务
        if(step.getIsBlockable()){
            int blockMinutes = TimeParser.parseMinutes(step.getTimeRequirement().getDuration());

            // set second for testing
            ScheduledFuture<?> future = scheduler.schedule(() -> {
                // 时间到后自动执行
                this.finishBlockable(sid, curRecipeIdx);
            }, blockMinutes, TimeUnit.SECONDS);

//                ScheduledFuture<?> future = scheduler.schedule(() -> {
//                    // 时间到后自动执行
//                    this.finishBlockable(sid, curRecipeIdx);
//                }, blockMinutes, TimeUnit.MINUTES);
            cookingRuntime.getTaskMap()
                    .put("RecipeIdx: " + String.valueOf(curRecipeIdx) + " StepIdx: " + String.valueOf(curStepIdx), future);
        } else {
            return false;
        }

        // 可以开始做下一道菜
        cookingRuntime.setCurrentRecipeIndex(curRecipeIdx + 1);

        return true;
    }

    public void finishBlockable(String sid, int curRecipeIdx){
        System.out.println("finishBlockable sid: "+ sid + "curReciprIdx:" + curRecipeIdx);
        CookingRuntime cookingRuntime = cookingMap.get(sid);
        if(cookingRuntime == null) return;

        cookingRuntime.setCurrentRecipeIndex(curRecipeIdx);
        List<Recipe> recipes = cookingRuntime.getRecipes();

        Recipe curRecipe = recipes.get(curRecipeIdx);
        String dishName = curRecipe.getDishName();

        Map<Integer,Integer> stepMap = cookingRuntime.getStepMap();
        // 取当前一步
        int curStepIdx = stepMap.get(curRecipeIdx);

        Step step = curRecipe.getSteps().get(curStepIdx);

        WsHandler.sendToWsSession(sid, "BLOCK_FINISHED: " + toJsonStep(dishName, step));
    }


    private Boolean currentRecipeExist(String sid) {
        CookingRuntime cookingRuntime = cookingMap.get(sid);
        List<Recipe> recipes = cookingRuntime.getRecipes();
        int curRecipeIdx = cookingRuntime.getCurrentRecipeIndex();

        if(curRecipeIdx >= recipes.size()){
            return false;
        }
        return true;
    }

    /*
    如果有下一步，指针指向下一步，并返回true
    否则直接返回false
     */
    private Boolean gotoNextStepifPresent (String sid){
        CookingRuntime cookingRuntime = cookingMap.get(sid);
        List<Recipe> recipes = cookingRuntime.getRecipes();
        int curRecipeIdx = cookingRuntime.getCurrentRecipeIndex();

        if(curRecipeIdx >= recipes.size()){
            // 当前没有下一步了
            return false;
        }

        Map<Integer,Integer> stepMap = cookingRuntime.getStepMap();
        // 取下一步
        int curStepIdx = stepMap.get(curRecipeIdx) + 1;
        // 当前的菜做完了，进入下一道
        while (curRecipeIdx < recipes.size() &&  curStepIdx  >= recipes.get(curRecipeIdx).getSteps().size()){
            curRecipeIdx++;
            if(curRecipeIdx < recipes.size()) {
                curStepIdx = stepMap.get(curRecipeIdx) + 1;
            }
        }
        // 遍历完了要做的菜也没找到下一步
        if(curRecipeIdx >= recipes.size()){
            return false;
        }
        // 找到了,设置当前步的信息
        cookingRuntime.setCurrentRecipeIndex(curRecipeIdx);
        stepMap.put(curRecipeIdx, curStepIdx);
        return true;
    }

    private String toJsonStep(String recipeName, Step step){
        ObjectNode node = M.createObjectNode().put("dishName", recipeName);
        JsonNode stepNode = M.valueToTree(step);
        node.setAll((ObjectNode) stepNode);
        return node.toString();
    }


    private Boolean currentStepIsBlockableButNotStart(String sid){
        CookingRuntime cookingRuntime = cookingMap.get(sid);
        List<Recipe> recipes = cookingRuntime.getRecipes();
        int curRecipeIdx = cookingRuntime.getCurrentRecipeIndex();
        Recipe recipe = recipes.get(curRecipeIdx);

        int curStepIdx = cookingRuntime.getStepMap().get(curRecipeIdx);
        // 还没开始
        if(curStepIdx==-1){
            return false;
        }
        if(recipe.getSteps().get(curStepIdx).getIsBlockable() &&
                (!cookingRuntime.getTaskMap().containsKey("RecipeIdx: " + String.valueOf(curRecipeIdx) + " StepIdx: " + String.valueOf(curStepIdx)))){
            return true;
        }
        // 如果是blockable且已经做过，删掉记录
        if(recipe.getSteps().get(curStepIdx).getIsBlockable()){
            cookingRuntime.getTaskMap().remove("RecipeIdx: " + String.valueOf(curRecipeIdx) + " StepIdx: " + String.valueOf(curStepIdx));
        }
        return false;
    }

}
