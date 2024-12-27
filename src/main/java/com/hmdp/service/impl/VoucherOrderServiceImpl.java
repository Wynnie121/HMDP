package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIP;
    static {
        SECKILL_SCRIP = new DefaultRedisScript<>();
        SECKILL_SCRIP.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIP.setResultType(Long.class);
    }

    private BlockingDeque<VoucherOrder> orderTasks = new LinkedBlockingDeque<>(1024 * 1024);
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //前置处理器
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        /**
         * 阻塞队列
         */
        @Override
        public void run() {
            while (true) {
                //获取redis消息队列的订单信息
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block((Duration.ofSeconds(2))),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断是否获取成功
                    if (list.isEmpty() || list == null) {
                        //获取失败没有消息，继续下一次循环
                        continue;
                    }
                    //获取消息，可以下单
                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    //没有被ACK
                    log.error("处理订单异常：", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                //获取redis消息队列的订单信息
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block((Duration.ofSeconds(2))),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断是否获取成功
                    if (list.isEmpty() || list == null) {
                        //没有异常消息，结束循环
                        break;
                    }
                    //获取消息，可以下单
                    //解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    //没有被ACK
                    log.error("处理pending-list订单异常：", e);
                }
            }
        }
    }
//    private class VoucherOrderHandler implements Runnable {
//
//        /**
//         * 阻塞队列
//         */
//        @Override
//        public void run() {
//            while(true){
//                //获取队列的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常：", e);
//                }
//                //创建订单
//            }
//        }
//    }
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象 获取锁
        RLock lock = redissonClient.getLock("order:" + userId);

        Boolean isLock = lock.tryLock();
        if(!isLock) {
            //重试或者返回错误信息
            log.error("不允许重复下单！");
            return;
        }
        //获取代理对象
        try {
            //子线程获取不到代理对象
            proxy.createVoucherOrder(voucherOrder); //非代理对象
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    /**
     * redis Stream消息队列
     * @param voucherId
     * @return
     */
    @Transactional
    public Result seckillVoucher(Long voucherId) {

        Long userId =  UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");

        //执行lua脚本-- 判断资格，发送消息
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIP,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), orderId.toString()
        );
        //判断结果
        int r = result.intValue();
        //不为0，没有购买资格
        if(r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "用户已经购买过一次");
        }
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);

    }


    /**
     * 阻塞队列
     * @param voucherOrder
     */
//    @Transactional
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        Long userId =  UserHolder.getUser().getId();
//
//        //执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIP,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        //判断结果
//        int r = result.intValue();
//        //不为0，没有购买资格
//        if(r != 0) {
//            return Result.fail(r == 1 ? "库存不足" : "用户已经购买过一次");
//        }
//        //有购买资格，把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        Long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        //创建阻塞队列
//        orderTasks.add(voucherOrder);
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //返回订单id
//        return Result.ok(orderId);
//
//    }


//    @Transactional
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        //查询信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀是否开始结束
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) return Result.fail("秒杀尚未开始！");
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())) return Result.fail("秒杀结束！");
//        //判断库存是否充足
//        if(voucher.getStock() < 1) return Result.fail("库存不足");
//
//        Long userId = UserHolder.getUser().getId();
//
//        //创建锁对象 获取锁
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("order:" + userId);
//
//        Boolean isLock = lock.tryLock();
//        if(!isLock) {
//            //重试或者返回错误信息
//            return Result.fail("一个人只允许下一单！");
//        }
//        //获取代理对象
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId); //非代理对象
//        } finally {
//            lock.unlock();
//        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //一人一单
        Long userId = voucherOrder.getUserId();

        //intern 在字符串池里找有没有一样的
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if(count > 0){
            log.error("用户已购买一次");
            return;
        }

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if(!success){
            log.error("库存不足");
            return;
        }

        save(voucherOrder);
//        return Result.ok(voucherOrder.getVoucherId());
    }
}
