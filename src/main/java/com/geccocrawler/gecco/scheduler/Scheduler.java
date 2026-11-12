package com.geccocrawler.gecco.scheduler;

import com.geccocrawler.gecco.request.HttpRequest;

/**
 * 需要下载的请求都放在这里管理，可以认为这里是一个队列，保存了所有待抓取的请求。系统默认采用FIFO的方式管理请求。
 * 
 * @author huchengyi
 *
 */
public interface Scheduler {

	public HttpRequest out();

	public void into(HttpRequest request);

	/**
	 * 调度器中是否还有待处理的请求。
	 * 默认返回false以保持对自定义Scheduler的向后兼容；
	 * 内置实现会给出准确结果，用于非循环模式的结束判定。
	 *
	 * @return true表示当前没有待处理请求
	 */
	default boolean isEmpty() {
		return false;
	}

}
