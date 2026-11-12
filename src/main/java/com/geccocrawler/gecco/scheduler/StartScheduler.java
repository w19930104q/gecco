package com.geccocrawler.gecco.scheduler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.geccocrawler.gecco.request.HttpRequest;

/**
 * 阻塞队列
 *
 * @author huchengyi
 *
 */
public class StartScheduler implements ShutdownableScheduler {

	private final LinkedBlockingQueue<HttpRequest> startQueue;

	private volatile boolean shutdown = false;

	public StartScheduler() {
		startQueue = new LinkedBlockingQueue<HttpRequest>();
	}

	@Override
	public HttpRequest out() {
		try {
			//使用带超时的poll，保证engineStop()后等待中的Spider线程能够周期性醒来并退出，
			//不会永久阻塞在take()上
			HttpRequest request = startQueue.poll(200L, TimeUnit.MILLISECONDS);
			while(request == null && !shutdown) {
				request = startQueue.poll(200L, TimeUnit.MILLISECONDS);
			}
			return request;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
	}

	@Override
	public void into(HttpRequest request) {
		try {
			startQueue.put(request);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public boolean isEmpty() {
		return startQueue.isEmpty();
	}

	@Override
	public void shutdown() {
		shutdown = true;
	}

	@Override
	public boolean isShutdown() {
		return shutdown;
	}

}
