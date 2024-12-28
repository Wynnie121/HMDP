package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hmdp.utils.RedisConstants.*;

//import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询店铺信息
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) throws InterruptedException {
        //缓存穿透
//        cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//         Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 10L, TimeUnit.MINUTES);
        if (shop == null) { return Result.fail("店铺不存在！");}
        //返回
        return Result.ok(shop);
    }


    /**
     * 缓存击穿----互斥锁
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否命中空值
        if(shopJson != null){
            //返回错误信息
            return null;
        }
        //未命中，实现缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取成功
            if(!isLock){
                //失败休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //成功查数据库
            shop = getById(id);
            Thread.sleep(200); //模拟延时
            //不存在写空值，解决缓存穿透
            if(shop == null){
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在将数据写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unlock(lockKey);
        }
        //返回
        return shop;
    }

    /**
     * 缓存击穿----逻辑过期
     * @param id
     * @return
     */
    public Shop queryWithExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        //从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断存在
        if(StrUtil.isBlank(shopJson)){
            //未命中
            return null;
        }

        //命中，把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期返回信息
            return shop;
        }

        //过期，尝试获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            //获取到。开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }
        //失败
        //返回店铺信息
        return shop;
    }

    /**
     * 尝试获取锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 设置逻辑过期时间
     * @param id
     * @param expireSecond
     */
    public void saveShop2Redis(Long id, Long expireSecond) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSecond));
        //写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.LOCK_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 先更新数据库，再删除缓存
     * @param shop
     * @return
     */
    @Transactional
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) { return Result.fail("店铺id不能为空！");}
        String key = CACHE_SHOP_KEY + id;
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {

        //判断是否需要根据坐标查询
        if(x == null || y == null){
            //不需要查
            Page<Shop> page = query()
                .eq("type_id", typeId)
                .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        }
        //计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //查询redis 排序分页 shopid distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        if(results == null){
            return Result.ok();
        }
        //解析出id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from)
        {
            return Result.ok(Collections.emptyList());
            //截取from-end
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.parseLong(shopIdStr));
            //距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        //查询店铺
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //返回

        return Result.ok(shops);
    }
}
