package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Transactional
    @Override
    public Result seckillVoucher(Long voucherId) {

        //查询信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始结束
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) return Result.fail("秒杀尚未开始！");
        if(voucher.getEndTime().isBefore(LocalDateTime.now())) return Result.fail("秒杀结束！");
        //判断库存是否充足
        if(voucher.getStock() < 1) return Result.fail("库存不足");

        Long userId = UserHolder.getUser().getId();

        //创建锁对象 获取锁
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

        Boolean isLock = lock.tryLock(12L);
        if(!isLock) {
            //重试或者返回错误信息
            return Result.fail("一个人只允许下一单！");
        }
        //获取代理对象
        try {
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId); //非代理对象
        } finally {
            lock.unlock();
        }

    }

    @Transactional
    public Result createVoucherOrder(Long voucherId){
        //一人一单
        Long userId = UserHolder.getUser().getId();

        //intern 在字符串池里找有没有一样的
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if(count > 0) return Result.fail("用户已购买过一次");

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0).update();
        if(!success) return Result.fail("库存不足");

        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        Long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }
}
