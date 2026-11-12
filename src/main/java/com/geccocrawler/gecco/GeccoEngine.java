package com.geccocrawler.gecco;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.geccocrawler.gecco.downloader.proxy.FileProxys;
import com.geccocrawler.gecco.downloader.proxy.Proxys;
import com.geccocrawler.gecco.dynamic.DynamicGecco;
import com.geccocrawler.gecco.dynamic.GeccoClassLoader;
import com.geccocrawler.gecco.listener.EventListener;
import com.geccocrawler.gecco.monitor.GeccoJmx;
import com.geccocrawler.gecco.monitor.GeccoMonitor;
import com.geccocrawler.gecco.pipeline.PipelineFactory;
import com.geccocrawler.gecco.request.HttpGetRequest;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.request.StartRequestList;
import com.geccocrawler.gecco.scheduler.NoLoopStartScheduler;
import com.geccocrawler.gecco.scheduler.RunRequestBoundary;
import com.geccocrawler.gecco.scheduler.Scheduler;
import com.geccocrawler.gecco.scheduler.ShutdownableScheduler;
import com.geccocrawler.gecco.scheduler.StartScheduler;
import com.geccocrawler.gecco.spider.Spider;
import com.geccocrawler.gecco.spider.SpiderBeanFactory;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * 爬虫引擎，每个爬虫引擎最好独立进程，在分布式爬虫场景下，可以单独分配一台爬虫服务器。引擎包括Scheduler、Downloader、Spider、 SpiderBeanFactory4个主要模块
 * 
 * @author huchengyi
 *
 */
public class GeccoEngine<V> extends Thread implements Callable<V> {

	private static Log log = LogFactory.getLog(GeccoEngine.class);

	private Date startTime;

	private List<HttpRequest> startRequests = new ArrayList<HttpRequest>();

	private Scheduler scheduler;

	/**
	 * 每次engine run建立的请求身份边界，保证同一次运行中相同GET URL只处理一次，
	 * 不同run之间相互隔离
	 */
	private volatile RunRequestBoundary requestBoundary;

	private SpiderBeanFactory spiderBeanFactory;

	private PipelineFactory pipelineFactory;

	private List<Spider> spiders;

	private String classpath;

	private int threadCount;

	private CountDownLatch cdl;

	private int interval;

	private Proxys proxysLoader;
	
	private boolean proxy = true;

	private boolean loop;

	private boolean mobile;

	private boolean debug;
	
	private boolean monitor = true;

	private int retry;

	private EventListener eventListener;
	
	private String jmxPrefix;

	private V ret;//callable 返回值

	public V getRet() {
		return ret;
	}

	public void setRet(V ret) {
		this.ret = ret;
	}

	private GeccoEngine() {
		this.retry = 3;
	}

	/**
	 * 动态配置规则不能使用该方法构造GeccoEngine
	 * 
	 * @return GeccoEngine
	 */
	public static GeccoEngine create() {
		GeccoEngine geccoEngine = new GeccoEngine();
		geccoEngine.setName("GeccoEngine");
		return geccoEngine;
	}

	public static GeccoEngine create(String classpath) {
		return create(classpath, null);
	}

	public static GeccoEngine create(String classpath, PipelineFactory pipelineFactory) {
		if (StringUtils.isEmpty(classpath)) {
			// classpath不为空
			throw new IllegalArgumentException("classpath cannot be empty");
		}
		GeccoEngine ge = create();
		ge.spiderBeanFactory = new SpiderBeanFactory(classpath, pipelineFactory);
		return ge;
	}

	@Deprecated
	public GeccoEngine start(String url) {
		return seed(new HttpGetRequest(url));
	}

	@Deprecated
	public GeccoEngine start(String... urls) {
		for (String url : urls) {
			seed(url);
		}
		return this;
	}

	@Deprecated
	public GeccoEngine start(HttpRequest request) {
		this.startRequests.add(request);
		return this;
	}

	@Deprecated
	public GeccoEngine start(List<HttpRequest> requests) {
		for (HttpRequest request : requests) {
			seed(request);
		}
		return this;
	}

	public GeccoEngine seed(String url) {
		return seed(new HttpGetRequest(url));
	}

	public GeccoEngine seed(String... urls) {
		for (String url : urls) {
			seed(url);
		}
		return this;
	}

	public GeccoEngine seed(HttpRequest request) {
		this.startRequests.add(request);
		return this;
	}

	public GeccoEngine seed(List<HttpRequest> requests) {
		for (HttpRequest request : requests) {
			seed(request);
		}
		return this;
	}

	public GeccoEngine scheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
		return this;
	}

	public GeccoEngine thread(int count) {
		this.threadCount = count;
		return this;
	}

	public GeccoEngine interval(int interval) {
		this.interval = interval;
		return this;
	}

	public GeccoEngine retry(int retry) {
		this.retry = retry;
		return this;
	}

	public GeccoEngine loop(boolean loop) {
		this.loop = loop;
		return this;
	}

	public GeccoEngine proxysLoader(Proxys proxysLoader) {
		this.proxysLoader = proxysLoader;
		return this;
	}
	
	public GeccoEngine proxy(boolean proxy) {
		this.proxy = proxy;
		return this;
	}

	public GeccoEngine mobile(boolean mobile) {
		this.mobile = mobile;
		return this;
	}

	public GeccoEngine debug(boolean debug) {
		this.debug = debug;
		return this;
	}
	
	public GeccoEngine monitor(boolean monitor) {
		this.monitor = monitor;
		return this;
	}

	public GeccoEngine classpath(String classpath) {
		this.classpath = classpath;
		return this;
	}
	
	public GeccoEngine jmxPrefix(String jmxPrefix) {
		this.jmxPrefix = jmxPrefix;
		return this;
	}

	public GeccoEngine pipelineFactory(PipelineFactory pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
		return this;
	}

	public GeccoEngine spiderBeanFactory(SpiderBeanFactory spiderBeanFactory) {
		this.spiderBeanFactory = spiderBeanFactory;
		return this;
	}

	public void register(Class<?> spiderBeanClass) {
		getSpiderBeanFactory().addSpiderBean(spiderBeanClass);
	}

	public void unregister(Class<?> spiderBeanClass) {
		getSpiderBeanFactory().removeSpiderBean(spiderBeanClass);
		DynamicGecco.unregister(spiderBeanClass);
	}

	@Override
	public void run() {
		if (debug) {
			Logger log = LogManager.getLogger("com.geccocrawler.gecco.spider.render");
			log.setLevel(Level.DEBUG);
		}
		if(proxysLoader == null) {//默认采用proxys文件代理集合
			proxysLoader = new FileProxys();
		}
		if (scheduler == null) {
			if (loop) {
				scheduler = new StartScheduler();
			} else {
				scheduler = new NoLoopStartScheduler();
			}
		}
		if (spiderBeanFactory == null) {
			if (StringUtils.isEmpty(classpath)) {
				// classpath不为空
				throw new IllegalArgumentException("classpath cannot be empty");
			}
			spiderBeanFactory = new SpiderBeanFactory(classpath, pipelineFactory);
		}
		if (threadCount <= 0) {
			threadCount = 1;
		}
		// 每次engine run建立独立的请求身份边界，多次run之间互不影响
		this.requestBoundary = new RunRequestBoundary();
		this.cdl = new CountDownLatch(threadCount);
		startsJson();
		if (startRequests.isEmpty()) {
			// startRequests不为空
			// throw new IllegalArgumentException("startRequests cannot be empty");
		}
		for (HttpRequest startRequest : startRequests) {
			scheduler.into(startRequest);
		}
		spiders = new ArrayList<Spider>(threadCount);
		for (int i = 0; i < threadCount; i++) {
			Spider spider = new Spider(this);
			spiders.add(spider);
			Thread thread = new Thread(spider, "T" + classpath + i);
			thread.start();
		}
		startTime = new Date();
		if(monitor) {
			// 监控爬虫基本信息
			GeccoMonitor.monitor(this);
			// 启动导出jmx信息
			GeccoJmx.export(jmxPrefix == null ? classpath : jmxPrefix);
		}
		if (!loop) {
			// 非循环模式：所有请求处理完且各队列都安静下来后关闭start调度器，
			// 唤醒等待中的Spider线程正常退出，避免线程永久自旋
			startQuiescenceWatcher();
		}
		// 非循环模式等待线程执行完毕后关闭
		closeUnitlComplete();
	}

	/**
	 * 非循环模式的安静期监听：当身份边界中没有在途请求、start队列与所有Spider派生队列
	 * 都为空时，关闭start调度器结束本次运行。
	 */
	private void startQuiescenceWatcher() {
		Thread watcher = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (!Thread.currentThread().isInterrupted()) {
						Thread.sleep(100L);
						if (isQuiescent()) {
							// 再观察一个周期，防止请求在"完成"与"派生入队"之间的瞬间被误判
							Thread.sleep(100L);
							if (isQuiescent()) {
								shutdownNoLoopScheduler();
								return;
							}
						}
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}

			private boolean isQuiescent() {
				if (requestBoundary == null || requestBoundary.getInFlight() != 0) {
					return false;
				}
				if (scheduler != null && !scheduler.isEmpty()) {
					return false;
				}
				if (spiders != null) {
					for (Spider spider : spiders) {
						if (!spider.getSpiderScheduler().isEmpty()) {
							return false;
						}
					}
				}
				return true;
			}
		}, "GeccoQuiescenceWatcher");
		watcher.setDaemon(true);
		watcher.start();
	}

	@Override
	public synchronized void start() {
		if (eventListener != null) {
			eventListener.onStart(this);
		}
		super.start();
	}

	private GeccoEngine startsJson() {
		try {
			URL url = Resources.getResource("starts.json");
			File file = new File(url.getPath());
			if (file.exists()) {
				String json = Files.toString(file, Charset.forName("UTF-8"));
				List<StartRequestList> list = JSON.parseArray(json, StartRequestList.class);
				for (StartRequestList start : list) {
					start(start.toRequest());
				}
			}
		} catch (IllegalArgumentException ex) {
			log.info("starts.json not found");
		} catch (IOException ioex) {
			log.error(ioex);
		}
		return this;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	/**
	 * 获取本次运行的请求身份边界。边界在每次run()开始时重建，
	 * 因此多次engine run之间的认领记录天然隔离。
	 *
	 * @return 当前运行的请求身份边界
	 */
	public RunRequestBoundary getRequestBoundary() {
		return requestBoundary;
	}

	public SpiderBeanFactory getSpiderBeanFactory() {
		return spiderBeanFactory;
	}

	public int getInterval() {
		return interval;
	}

	public Date getStartTime() {
		return startTime;
	}

	public List<HttpRequest> getStartRequests() {
		return startRequests;
	}

	public List<Spider> getSpiders() {
		return spiders;
	}

	public int getRetry() {
		return retry;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public boolean isLoop() {
		return loop;
	}

	/**
	 * 非循环模式下，start调度器是否已经关闭。
	 * 关闭后Spider线程拿到null即可安全退出，而不是继续等待新种子。
	 *
	 * @return true表示start调度器已关闭
	 */
	public boolean isStartSchedulerShutdown() {
		if (scheduler instanceof ShutdownableScheduler) {
			return ((ShutdownableScheduler) scheduler).isShutdown();
		}
		return false;
	}

	public Proxys getProxysLoader() {
		return proxysLoader;
	}

	public boolean isMobile() {
		return mobile;
	}

	public boolean isDebug() {
		return debug;
	}
	
	public boolean isProxy() {
		return proxy;
	}
	
	public boolean isMonitor() {
		return monitor;
	}

	/**
	 * spider线程告知engine执行结束
	 */
	public void notifyComplete() {
		this.cdl.countDown();
	}

	/**
	 * 非循环模式等待线程执行完毕后关闭
	 */
	public void closeUnitlComplete() {
		if (!loop) {
			try {
				cdl.await();
			} catch (InterruptedException e) {
				log.error(e);
			}
			shutdownNoLoopScheduler();
			if (spiderBeanFactory != null) {
				spiderBeanFactory.getDownloaderFactory().closeAll();
			}
			GeccoJmx.unexport();
			log.info("close gecco!");
		}

		if (eventListener != null) {
			eventListener.onStop(this);
		}
	}

	/**
	 * 启动引擎，并返回GeccoEngine对象
	 * 
	 * @return GeccoEngine
	 */
	public GeccoEngine engineStart() {
		start();
		return this;
	}

	/**
	 * 暂停
	 */
	public void pause() {
		if (spiders != null) {
			for (Spider spider : spiders) {
				spider.pause();
			}
		}
		if (eventListener != null) {
			eventListener.onPause(this);
		}
	}

	/**
	 * 重新开始抓取
	 */
	public void restart() {
		if (spiders != null) {
			for (Spider spider : spiders) {
				spider.restart();
			}
		}
		if (eventListener != null) {
			eventListener.onRestart(this);
		}
	}

	public void beginUpdateRule() {
		if (log.isDebugEnabled()) {
			log.debug("begin update rule");
		}
		// 修改规则前需要暂停引擎并且重新创建ClassLoader
		pause();
		GeccoClassLoader.create();
	}

	public void endUpdateRule() {
		// 修改完成后重启引擎
		restart();
		if (log.isDebugEnabled()) {
			log.debug("end update rule");
		}
	}

	public void engineStop() {
		if (spiders != null) {
			for (Spider spider : spiders) {
				spider.stop();
			}
		}
		shutdownScheduler();
		if (eventListener != null) {
			eventListener.onStop(this);
		}
	}

	public EventListener getEventListener() {
		return eventListener;
	}

	public GeccoEngine setEventListener(EventListener eventListener) {
		this.eventListener = eventListener;
		return this;
	}

	private void shutdownNoLoopScheduler() {
		if (!loop) {
			shutdownScheduler();
		}
	}

	/**
	 * 关闭start调度器，唤醒可能在out()中等待的Spider线程使其感知停止信号
	 */
	private void shutdownScheduler() {
		if (scheduler instanceof ShutdownableScheduler) {
			((ShutdownableScheduler) scheduler).shutdown();
		}
	}

	@Override
	public V call() throws Exception {
		run();
		return ret;
	}
}
