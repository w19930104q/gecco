package com.geccocrawler.gecco.dedup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.geccocrawler.gecco.GeccoEngine;
import com.geccocrawler.gecco.monitor.DownloadMonitor;
import com.geccocrawler.gecco.request.HttpGetRequest;
import com.geccocrawler.gecco.request.HttpPostRequest;
import com.geccocrawler.gecco.request.HttpRequest;

/**
 * 请求身份边界回归测试，覆盖：
 * seed重复、菱形页、多worker同URL、POST差异、重定向、失败重排、多次engine隔离，
 * 并验证自定义Scheduler、线程数、循环/非循环、pipeline及JMX事件语义不被破坏。
 */
public class RunDedupHttpTest {

    private static final String CLASSPATH = "com.geccocrawler.gecco.dedup";

    private LocalHttpServer server;

    @Before
    public void setUp() throws Exception {
        server = new LocalHttpServer();
        RecordPipeline.reset();
        FlakyDownloader.reset(2);
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private GeccoEngine nonLoopEngine(String jmxPrefix) {
        return GeccoEngine.create(CLASSPATH)
                .jmxPrefix(jmxPrefix)
                .loop(false)
                .thread(4)
                .interval(0)
                .retry(3);
    }

    /**
     * 本次测试服务的base url（含唯一端口），用于把全局pipeline记录限定到本用例，
     * 避免其它用例残留记录干扰断言
     */
    private String base() {
        return server.url("");
    }

    /**
     * 1. seed重复：同一GET URL多次seed，同一次运行只成功处理一次
     */
    @Test
    public void duplicateSeedDownloadedOnce() throws Exception {
        String url = server.url("/seed");
        GeccoEngine engine = nonLoopEngine("dedup-seed")
                .seed(url).seed(url).seed(new HttpGetRequest(url));
        engine.run();
        assertEquals(1, server.getHitCount("GET", "/seed"));
        assertEquals(1, RecordPipeline.countContaining(base()));
        assertTrue(RecordPipeline.getProcessed().contains("GET " + url));
    }

    /**
     * 2. 菱形页：两个不同页面分别派生指向同一地址的请求，
     * 可能进入不同Spider线程的私有队列，下游只下载一次
     */
    @Test
    public void diamondPageDerivedDuplicateDownloadedOnce() throws Exception {
        String leafUrl = server.url("/diamond/leaf");
        String linkHtml = "<html><body><h1>diamond</h1>"
                + "<a class=\"leaf\" href=\"" + leafUrl + "\">leaf</a>"
                + "</body></html>";
        server.html("/diamond/a", linkHtml);
        server.html("/diamond/b", linkHtml);
        GeccoEngine engine = nonLoopEngine("dedup-diamond").thread(4)
                .seed(server.url("/diamond/a"))
                .seed(server.url("/diamond/b"));
        engine.run();
        assertEquals(1, server.getHitCount("GET", "/diamond/leaf"));
        assertEquals(1, server.getHitCount("GET", "/diamond/a"));
        assertEquals(1, server.getHitCount("GET", "/diamond/b"));
    }

    /**
     * 3. 多worker同URL：多个线程并发竞争同一URL（菱形+重复seed混合），仍然只下载一次
     */
    @Test
    public void multipleWorkersSameUrlDownloadedOnce() throws Exception {
        String sharedUrl = server.url("/shared");
        String linkHtml = "<html><body><h1>diamond</h1>"
                + "<a class=\"leaf\" href=\"" + sharedUrl + "\">a</a>"
                + "</body></html>";
        server.html("/diamond/a", linkHtml);
        server.html("/diamond/b", linkHtml);
        GeccoEngine engine = nonLoopEngine("dedup-workers").thread(8)
                .seed(server.url("/diamond/a"))
                .seed(server.url("/diamond/b"))
                .seed(sharedUrl)
                .seed(sharedUrl);
        engine.run();
        assertEquals(1, server.getHitCount("GET", "/shared"));
        assertEquals(1, server.getHitCount("GET", "/diamond/a"));
        assertEquals(1, server.getHitCount("GET", "/diamond/b"));
    }

    /**
     * 4. POST差异：相同URL的POST，body不同保持独立；body相同仍然去重
     */
    @Test
    public void postDifferentBodyKeptIndependent() throws Exception {
        String url = server.url("/post");
        HttpPostRequest postA = new HttpPostRequest(url);
        postA.addField("a", "1");
        HttpPostRequest postADup = new HttpPostRequest(url);
        postADup.addField("a", "1");
        HttpPostRequest postB = new HttpPostRequest(url);
        postB.addField("a", "2");
        GeccoEngine engine = nonLoopEngine("dedup-post")
                .seed(postA).seed(postADup).seed(postB);
        engine.run();
        // 相同body的POST只处理一次，不同body各自处理
        assertEquals(2, server.getHitCount("POST", "/post"));
        assertEquals(2, RecordPipeline.countContaining(base()));
    }

    /**
     * 5. 重定向：302跳转到已处理过的地址时，不再拉取目标正文
     */
    @Test
    public void redirectToProcessedUrlSkipsBody() throws Exception {
        String target = server.url("/redirect/target");
        server.redirect("/redirect/from", target);
        GeccoEngine engine = nonLoopEngine("dedup-redirect")
                // 先处理目标地址，再让另一个seed重定向到同一地址
                .seed(target)
                .seed(server.url("/redirect/from"));
        engine.run();
        // 目标正文只下载一次；重定向地址只产生一次302响应
        assertEquals(1, server.getHitCount("GET", "/redirect/target"));
        assertEquals(1, server.getHitCount("GET", "/redirect/from"));
    }

    /**
     * 6. 失败重排：超时等可重试失败释放/重排认领，最终重试成功；
     * 重试期间该请求不会被当作完成，也不会被重复并发下载
     */
    @Test
    public void retryableFailureRequeuedAndSucceeds() throws Exception {
        FlakyDownloader.reset(2);
        String url = server.url("/flaky");
        GeccoEngine engine = nonLoopEngine("dedup-retry")
                .thread(4)
                .retry(3)
                .seed(url).seed(url);
        engine.run();
        // 前2次注入超时，第3次成功；重复seed不会产生额外下载
        assertEquals(3, FlakyDownloader.getAttempts());
        assertEquals(1, server.getHitCount("GET", "/flaky"));
        assertEquals(1, RecordPipeline.countContaining(base()));
    }

    /**
     * 6b. 重试耗尽：超过重试次数后请求终止，不会无限重排
     */
    @Test
    public void retriesExhaustedTerminatesRequest() throws Exception {
        FlakyDownloader.reset(10);
        String url = server.url("/flaky");
        GeccoEngine engine = nonLoopEngine("dedup-retry-exhaust")
                .retry(2)
                .seed(url);
        engine.run();
        // 首次 + 2次重试 = 3次尝试，之后终止
        assertEquals(3, FlakyDownloader.getAttempts());
        assertEquals(0, server.getHitCount("GET", "/flaky"));
        assertEquals(0, RecordPipeline.countContaining(base()));
    }

    /**
     * 7. 多次engine隔离：两个独立engine run分别重建身份边界，同一URL各成功处理一次
     */
    @Test
    public void separateEngineRunsAreIsolated() throws Exception {
        String url = server.url("/isolate");
        GeccoEngine first = nonLoopEngine("dedup-isolate-1").seed(url);
        first.run();
        GeccoEngine second = nonLoopEngine("dedup-isolate-2").seed(url);
        second.run();
        assertEquals(2, server.getHitCount("GET", "/isolate"));
        assertEquals(2, RecordPipeline.countContaining(base()));
    }

    /**
     * 8. 自定义Scheduler语义保留：用户Scheduler正常收发请求，去重仍在身份边界生效
     */
    @Test
    public void customSchedulerStillWorks() throws Exception {
        String url = server.url("/custom");
        CustomTestScheduler scheduler = new CustomTestScheduler();
        GeccoEngine engine = GeccoEngine.create(CLASSPATH)
                .jmxPrefix("dedup-custom")
                .scheduler(scheduler)
                .loop(false)
                .thread(2)
                .interval(0)
                .retry(3)
                .seed(url).seed(url);
        engine.run();
        assertEquals(1, server.getHitCount("GET", "/custom"));
        assertTrue("custom scheduler should receive seeds, into=" + scheduler.getIntoCount(),
                scheduler.getIntoCount() >= 2);
        assertTrue("custom scheduler should dispatch requests, out=" + scheduler.getOutCount(),
                scheduler.getOutCount() >= 1);
    }

    /**
     * 9. JMX事件语义保留：非循环引擎结束后MBean被注销
     */
    @Test
    public void jmxLifecyclePreserved() throws Exception {
        String prefix = "dedup-jmx";
        GeccoEngine engine = nonLoopEngine(prefix).seed(server.url("/jmx"));
        engine.run();
        Set<ObjectName> names = ManagementFactory.getPlatformMBeanServer().queryNames(null, null);
        for (ObjectName name : names) {
            assertTrue("MBean should be unexported after non-loop run: " + name,
                    !name.getDomain().equals(prefix));
        }
    }

    /**
     * 10. pipeline与监控统计语义保留：成功/失败计数符合预期
     */
    @Test
    public void pipelineAndMonitorStatisticsPreserved() throws Exception {
        FlakyDownloader.reset(1);
        GeccoEngine engine = nonLoopEngine("dedup-monitor")
                .retry(3)
                .seed(server.url("/flaky"));
        engine.run();
        assertEquals(1, RecordPipeline.countContaining(base()));
        // 一次超时异常 + 一次成功
        assertTrue(DownloadMonitor.getStatistics("127.0.0.1").getException() >= 1);
        assertTrue(DownloadMonitor.getStatistics("127.0.0.1").getSuccess() >= 1);
    }

    /**
     * 11. 循环模式语义保留：loop=true时start请求重新入队，身份边界不阻止下一轮调度
     */
    @Test
    public void loopModeStillCycles() throws Exception {
        String url = server.url("/loop");
        final GeccoEngine engine = GeccoEngine.create(CLASSPATH)
                .jmxPrefix("dedup-loop")
                .loop(true)
                .thread(2)
                .interval(50)
                .retry(1)
                .seed(url);
        Thread runner = new Thread(new Runnable() {
            @Override
            public void run() {
                engine.run();
            }
        });
        runner.start();
        // 让循环引擎跑若干轮后停止
        Thread.sleep(1500L);
        engine.engineStop();
        runner.join(5000L);
        assertTrue("loop engine should hit the seed more than once",
                server.getHitCount("GET", "/loop") >= 2);
    }
}
