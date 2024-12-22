package com.hmdp.utils;

public interface ILock {

    Boolean tryLock(long timeoutSec);
    void unlock();

}
