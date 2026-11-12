package com.geccocrawler.gecco.scheduler;

/**
 * 支持显式关闭的调度器。非循环模式下，当所有请求都处理完成时，
 * GeccoEngine通过该接口关闭start调度器，告知等待中的Spider线程退出。
 *
 * @author gecco
 */
public interface ShutdownableScheduler extends Scheduler {

    /**
     * 关闭调度器，关闭后阻塞中的out()应当返回null而不是继续等待。
     */
    void shutdown();

    /**
     * @return true表示调度器已关闭
     */
    boolean isShutdown();
}
