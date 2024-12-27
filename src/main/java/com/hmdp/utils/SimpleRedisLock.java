package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultReactiveScriptExecutor;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.yaml.snakeyaml.events.Event;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    //业务名称
    private String name;

    private StringRedisTemplate stringRedisTemplate;
    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIP;
    static {
        UNLOCK_SCRIP = new DefaultRedisScript<>();
        UNLOCK_SCRIP.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIP.setResultType(Long.class);
    }


    @Override
    public Boolean tryLock(long timeoutSec) {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取redis锁里面的表示
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        //判断是否一致
        //调用lua脚本
        if(threadId.equals(id)) {
            stringRedisTemplate.execute(
                    UNLOCK_SCRIP,
                    Collections.singletonList(KEY_PREFIX + name),
                    ID_PREFIX + Thread.currentThread().getId());
        }
    }
}
