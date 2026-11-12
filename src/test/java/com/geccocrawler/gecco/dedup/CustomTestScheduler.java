package com.geccocrawler.gecco.dedup;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.scheduler.ShutdownableScheduler;

/**
 * 用户自定义Scheduler：验证修复不破坏自定义Scheduler语义。
 * 实现ShutdownableScheduler以便非循环模式正常退出，内部统计入队/出队次数。
 */
public class CustomTestScheduler implements ShutdownableScheduler {

    private final LinkedBlockingQueue<HttpRequest> queue = new LinkedBlockingQueue<HttpRequest>();
    private final AtomicInteger intoCount = new AtomicInteger(0);
    private final AtomicInteger outCount = new AtomicInteger(0);
    private volatile boolean shutdown = false;

    @Override
    public HttpRequest out() {
        try {
            HttpRequest request = queue.poll(200L, TimeUnit.MILLISECONDS);
            if (request != null) {
                outCount.incrementAndGet();
            }
            return request;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public void into(HttpRequest request) {
        if (request == null) {
            return;
        }
        queue.offer(request);
        intoCount.incrementAndGet();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    public int getIntoCount() {
        return intoCount.get();
    }

    public int getOutCount() {
        return outCount.get();
    }
}
