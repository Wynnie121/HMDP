package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {
    @Bean
    public RedissonClient redisson() {
        // 配置类
        Config config = new Config();
        // 添加redis地址
        config.useSingleServer().setAddress("redis://localhost:6379").setPassword("123456");
        // 创建客户端
        return Redisson.create(config);
    }
}
