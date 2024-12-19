package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    String key = "cache:shoptype";
    @Resource
    StringRedisTemplate stringRedisTemplate;

    /**
     * redis缓存店铺类型
     * @return
     */
    @Override
    public List<ShopType> getTypeList() {
        String shopType = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopType)) {
            List<ShopType> shopTypeList = JSONUtil.toList(shopType, ShopType.class);
            return shopTypeList;
        }
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        if(shopTypeList ==null || shopTypeList.size()==0){
            return shopTypeList;
        }
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));
        return shopTypeList;

    }
}
