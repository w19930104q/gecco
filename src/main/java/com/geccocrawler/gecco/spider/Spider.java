package com.geccocrawler.gecco.spider;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.geccocrawler.gecco.GeccoEngine;
import com.geccocrawler.gecco.downloader.AfterDownload;
import com.geccocrawler.gecco.downloader.BeforeDownload;
import com.geccocrawler.gecco.downloader.Downloader;
import com.geccocrawler.gecco.downloader.DownloadException;
import com.geccocrawler.gecco.downloader.DownloadServerException;
import com.geccocrawler.gecco.pipeline.Pipeline;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.response.HttpResponse;
import com.geccocrawler.gecco.scheduler.RunRequestBoundary;
import com.geccocrawler.gecco.scheduler.Scheduler;
import com.geccocrawler.gecco.scheduler.UniqueSpiderScheduler;
import com.geccocrawler.gecco.spider.render.Render;

/**
 * 一个爬虫引擎可以包含多个爬虫，每个爬虫可以认为是一个单独线程，爬虫会从Scheduler中获取需要待抓取的请求。
 * 爬虫的任务就是下载网页并渲染相应的JavaBean。
 *
 * @author huchengyi
 *
 */
public class Spider implements Runnable {

	private static Log log = LogFactory.getLog(Spider.class);

	private CountDownLatch pauseCountDown;

	private volatile boolean stop;

	private volatile boolean pause;

	private GeccoEngine engine;

	private Scheduler spiderScheduler;

	/**
	 * 当前待渲染的bean
	 */
	public Class<? extends SpiderBean> currSpiderBeanClass;

	public Spider(GeccoEngine engine) {
		this.engine = engine;
		this.spiderScheduler = new UniqueSpiderScheduler();
		this.pause = false;
		this.stop = false;
	}

	public void run() {
		//将spider放入线程本地变量，之后需要使用
		SpiderThreadLocal.set(this);
		while(true) {
			//停止
			if(stop) {
				//告知engine线程执行结束
				engine.notifyComplete();
				break;
			}
			//暂停抓取
			if(pause) {
				try {
					this.pauseCountDown.await();
				} catch (InterruptedException e) {
					log.error("can't pause : ", e);
				}
			}
			//获取待抓取的url
			boolean start = false;
			boolean failed = false;
			HttpRequest request = spiderScheduler.out();
			if(request == null) {
				//startScheduler
				request = engine.getScheduler().out();
				if(request == null) {
					if (engine.isLoop()) {
						//循环模式下空队列意味着真正结束
						engine.notifyComplete();
						break;
					}
					// 非循环模式：start调度器关闭表示本次运行的请求都已处理完，退出；
					// 否则等待新的种子或派生请求（动态注入场景）
					if (engine.isStartSchedulerShutdown()) {
						engine.notifyComplete();
						break;
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
					continue;
				}
				start = true;
			}
			// 可重试失败后重新入队的具体请求实例可以直接恢复处理；
			// 相同身份的并发重复请求是不同实例，拿不到重试标记，仍需走统一认领
			boolean retry = engine.getRequestBoundary().takeRetry(request);
			if(!retry) {
				// 本地派生队列与start调度之间统一认领：
				// 同一次运行中相同身份的GET请求只有第一个能认领成功，
				// 重复请求（seed重复、菱形页面、多worker同URL）不会被下载
				if(!engine.getRequestBoundary().claim(request)) {
					continue;
				}
			}
			if(log.isDebugEnabled()) {
				log.debug("match url : " + request.getUrl());
			}
			//匹配SpiderBean
			currSpiderBeanClass = engine.getSpiderBeanFactory().matchSpider(request);
			//download
			HttpResponse response = null;
			try {
				if(currSpiderBeanClass == null) {//如果无法匹配但是是302跳转，需要放入抓取队列继续抓取
					response = defaultDownload(request);
					if(response.getStatus() == 302 || response.getStatus() == 301){
						intoDeriveQueue(request.subRequest(response.getContent()));
					} else {
						log.error("cant't match url : " + request.getUrl());
					}
				} else {
					//获取SpiderBean的上下文：downloader,beforeDownloader,afterDownloader,render,pipelines
					SpiderBeanContext context = getSpiderBeanContext();
					response = download(context, request);
					if(response.getStatus() == 200) {
						//render
						Render render = context.getRender();

						SpiderBean spiderBean = null;
						spiderBean = render.inject(currSpiderBeanClass, request, response);

						//pipelines
						pipelines(spiderBean, context);
					} else if(response.getStatus() == 302 || response.getStatus() == 301){
						//重定向到已处理过的地址时，身份边界会拒绝认领，不再拉取正文
						intoDeriveQueue(request.subRequest(response.getContent()));
					}
				}
				//下载及渲染流程走完（含302/301跳转、无法匹配等正常分支），标记本次身份处理完成
				engine.getRequestBoundary().complete(request);
			} catch(DownloadException ex) {
				//超时、连接失败等可重试失败：释放认领并重新排队，不能永久标记完成
				failed = true;
				handleDownloadFailure(request, ex, start);
			} catch(Exception ex) {
				if(engine.isDebug()) {
					log.error(request.getUrl() + " ERROR : ", ex);
				}
				log.error(request.getUrl() + " ERROR : " + ex.getClass().getName() + ex.getMessage());
				//渲染等非下载异常不具备重试语义，保留完成标记避免重复抓取
				engine.getRequestBoundary().complete(request);
			} finally {
				if(response != null) {
					response.close();
				}
			}
			//抓取间隔
			interval();
			//开始地址放入队尾重新抓取
			if(start && engine.isLoop() && !failed) {
				//循环模式：start种子本轮成功处理完后重新开放认领并放回start队列，
				//下一轮可以再抓取一次；同一轮内的重复请求仍被身份边界去重。
				//下载失败的请求已由handleDownloadFailure按重试路由处理，不在此重复入队
				engine.getRequestBoundary().reopen(request);
				engine.getScheduler().into(request);
			}
		}
	}

	/**
	 * 请求放入当前Spider的派生队列，由本次运行的身份边界统一去重
	 */
	private void intoDeriveQueue(HttpRequest request) {
		if(request == null) {
			return;
		}
		spiderScheduler.into(request);
	}

	/**
	 * 下载失败处理：超时、连接失败等可重试失败重新排队并保持认领可恢复，
	 * 重试次数耗尽后终止该请求；4xx/5xx等服务端响应属于确定性失败，不重试，
	 * 直接完成认领避免相同请求反复请求服务器。
	 * 重试请求回到它被认领的队列：start种子回到start队列，派生请求回到Spider私有队列。
	 *
	 * @param request 失败的请求
	 * @param ex      下载异常
	 * @param start   该请求是否来自start调度器
	 */
	private void handleDownloadFailure(HttpRequest request, DownloadException ex, boolean start) {
		if(engine.isDebug()) {
			log.error(request.getUrl() + " ERROR : ", ex);
		}
		log.error(request.getUrl() + " ERROR : " + ex.getClass().getName() + ex.getMessage());
		RunRequestBoundary boundary = engine.getRequestBoundary();
		if(ex instanceof DownloadServerException) {
			boundary.complete(request);
			return;
		}
		Scheduler retryScheduler = (start && engine.isLoop()) ? engine.getScheduler() : spiderScheduler;
		boolean requeued = boundary.requeue(retryScheduler, request, engine.getRetry());
		if(!requeued) {
			//重试次数耗尽，边界内部已将该身份标记为完成
			log.error(request.getUrl() + " dropped after " + engine.getRetry() + " retries");
		}
	}

	/**
	 * 暂停，当前正在抓取的请求会继续抓取完成，之后会等到restart的调用才继续抓取
	 */
	public void pause() {
		this.pauseCountDown = new CountDownLatch(1);
		this.pause = true;
	}

	/**
	 * 重新开始
	 */
	public void restart() {
		this.pauseCountDown.countDown();
		this.pause = false;
	}

	/**
	 * 停止抓取
	 */
	public void stop() {
		this.stop = true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void pipelines(SpiderBean spiderBean, SpiderBeanContext context) {
		if(spiderBean == null) {
			return ;
		}
		List<Pipeline> pipelines = context.getPipelines();
		if(pipelines != null) {
			for(Pipeline pipeline : pipelines) {
				pipeline.process(spiderBean);
			}
		}
	}

	private void interval() {
		int interval = engine.getInterval();
		if(interval > 0) {
			try {
				Thread.sleep(randomInterval(interval));
			} catch (InterruptedException e) {}
		}
	}

	/**
	 * 默认下载
	 *
	 * @param request
	 * @return
	 */
	private HttpResponse defaultDownload(HttpRequest request) throws DownloadException {
		HttpResponse response = download(null, request);
		return response;
	}

	private HttpResponse download(SpiderBeanContext context, HttpRequest request) throws DownloadException {
			Downloader currDownloader = null;
			BeforeDownload before = null;
			AfterDownload after = null;
			int timeout = 1000;
			if(context != null) {
				currDownloader = context.getDownloader();
				before = context.getBeforeDownload();
				after = context.getAfterDownload();
				timeout = context.getTimeout();
			} else {
				currDownloader = engine.getSpiderBeanFactory().getDownloaderFactory().defaultDownloader();
			}
			if(before != null) {
				before.process(request);
			}
			HttpResponse response = currDownloader.download(request, timeout);
			if(after != null) {
				after.process(request, response);
			}
			return response;
	}

	/**
	 * 间隔时间在左右1s的范围内随机
	 *
	 * @param interval
	 * @return
	 */
	private int randomInterval(int interval) {
		int min = interval - 1000;
		if(min < 1) {
			min = 1;
		}
		int max = interval + 1000;
		return (int)Math.rint(Math.random()*(max-min)+min);
	}

	public GeccoEngine getEngine() {
		return engine;
	}

	public Scheduler getSpiderScheduler() {
		return spiderScheduler;
	}

	public void setSpiderScheduler(Scheduler spiderScheduler) {
		this.spiderScheduler = spiderScheduler;
	}

	public SpiderBeanContext getSpiderBeanContext() {
		return engine.getSpiderBeanFactory().getContext(currSpiderBeanClass);
	}
}
