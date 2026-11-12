package com.geccocrawler.gecco.scheduler;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.geccocrawler.gecco.request.HttpPostRequest;
import com.geccocrawler.gecco.request.HttpRequest;

/**
 * 单次引擎运行(engine run)内并发安全的请求身份边界。
 *
 * 同一个GeccoEngine的一次run会启动多个Spider线程，初始种子由start调度器分发，
 * 页面派生的请求进入各Spider线程的私有队列，两个入口都可能出现相同请求。
 * 该边界在两个入口之间统一"认领"请求，保证：
 * <ul>
 * <li>同一次运行中，相同身份的GET请求只会被成功处理一次；</li>
 * <li>POST请求以url+body作为身份，body不同的请求相互独立，各自处理；</li>
 * <li>下载等可重试失败会释放认领并重新入队，不会被永久标记为完成；</li>
 * <li>不同的engine run之间相互隔离，互不影响。</li>
 * </ul>
 *
 * @author gecco
 */
public class RunRequestBoundary {

    private static Log log = LogFactory.getLog(RunRequestBoundary.class);

    /**
     * 请求在本次运行中的认领状态。
     */
    private enum State {
        /** 已认领、正在被某个Spider线程处理 */
        PROCESSING,
        /** 循环模式下start种子本轮完成、已重新入队，等待下一轮被重新认领一次 */
        OPEN,
        /** 已处理完成，本次运行中再次出现的相同身份请求直接跳过，不再下载 */
        DONE
    }

    /** key=请求身份，value=认领状态 */
    private final Map<String, State> states;

    /** key=请求身份，value=已经发生的可重试失败次数 */
    private final Map<String, Integer> failures;

    /**
     * 可重试失败后重新入队的请求实例标记（基于对象身份而非equals）。
     * 只有被标记的那个具体请求实例才是重试请求；并发出现的相同身份重复请求
     * 是不同实例，不会被误认为重试，从而仍然被去重。
     */
    private final Set<HttpRequest> retryMarkers =
            Collections.newSetFromMap(new ConcurrentHashMap<HttpRequest, Boolean>());

    /**
     * 已认领但尚未处理完成（含重试等待中、循环队列中等待下一轮）的请求数。
     * 非循环模式下该计数归零且所有队列都为空时，表示本次运行真正结束。
     */
    private final AtomicInteger inFlight = new AtomicInteger(0);

    public RunRequestBoundary() {
        this.states = new ConcurrentHashMap<String, State>();
        this.failures = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * 计算请求在本次运行中的身份。
     * GET请求只以url作为身份；POST请求额外包含按字段名排序后的请求体，
     * 保证body不同的POST互不影响，且不受字段插入顺序影响（不含Referer等头部）。
     *
     * @param request 待抓取请求
     * @return 请求身份
     */
    public static String identity(HttpRequest request) {
        if (request instanceof HttpPostRequest) {
            Map<String, String> fields = ((HttpPostRequest) request).getFields();
            return "POST " + request.getUrl() + " " + new java.util.TreeMap<String, String>(fields).toString();
        }
        return "GET " + request.getUrl();
    }

    /**
     * 尝试认领一个请求。只有本运行中第一次出现的身份、或循环模式下处于OPEN状态的
     * 下一轮种子才能认领成功，重复请求会被直接丢弃。
     *
     * @param request 待抓取请求
     * @return true表示认领成功可以处理；false表示该身份正在处理中或已处理完成
     */
    public boolean claim(HttpRequest request) {
        String key = identity(request);
        State existing = states.putIfAbsent(key, State.PROCESSING);
        if (existing == null) {
            inFlight.incrementAndGet();
            return true;
        }
        //循环模式下重新开放的种子，允许被重新认领一次
        if (existing == State.OPEN && states.replace(key, State.OPEN, State.PROCESSING)) {
            return true;
        }
        if (log.isDebugEnabled()) {
            log.debug("duplicate request skipped in this run : " + key);
        }
        return false;
    }

    /**
     * 请求处理完成（下载成功或不可重试的失败），保留认领信息并标记为DONE，
     * 使本次运行后续出现的相同身份请求不再被下载。
     *
     * @param request 已处理完成的请求
     */
    public void complete(HttpRequest request) {
        if (states.put(identity(request), State.DONE) != State.DONE) {
            inFlight.decrementAndGet();
        }
    }

    /**
     * 可重试失败时释放认领并重新排队。
     * 未超过maxRetries时，请求重新进入scheduler队列并打上重试标记，
     * 认领保持为PROCESSING，其它线程的重复请求仍会被拒绝，只有该重试实例能被再次处理；
     * 超过最大重试次数后标记为DONE，请求终止，避免无限重排。
     *
     * @param scheduler  目标调度器（Spider私有派生队列或start调度器）
     * @param request    处理失败的请求
     * @param maxRetries 最大重试次数（不含首次处理）
     * @return true表示已重新入队；false表示重试次数耗尽，请求终止
     */
    public boolean requeue(Scheduler scheduler, HttpRequest request, int maxRetries) {
        String key = identity(request);
        int times = failures.merge(key, 1, Integer::sum);
        if (times > maxRetries) {
            states.put(key, State.DONE);
            failures.remove(key);
            inFlight.decrementAndGet();
            log.error("request retries exhausted, give up : " + key);
            return false;
        }
        //先打标记再入队，保证请求被再次poll出来时一定能识别为重试实例
        retryMarkers.add(request);
        states.put(key, State.PROCESSING);
        scheduler.into(request);
        return true;
    }

    /**
     * 取出并清除请求实例上的重试标记。
     * 仅可重试失败后重新入队的那个具体请求实例会返回true，
     * 相同身份的并发重复请求（不同实例）仍然返回false。
     *
     * @param request 出队的请求
     * @return true表示这是一个等待再次处理的重试请求
     */
    public boolean takeRetry(HttpRequest request) {
        return retryMarkers.remove(request);
    }

    /**
     * 循环模式下，一个start请求完成本轮处理后重新放入start队列，
     * 需要为下一轮抓取重新开放认领。同一轮内派生的重复请求仍然保持去重，
     * 只有被显式reopen的start身份会在下一轮被再次处理一次。
     *
     * @param request 重新进入start队列的循环种子
     */
    public void reopen(HttpRequest request) {
        String key = identity(request);
        failures.remove(key);
        if (states.put(key, State.OPEN) == State.DONE) {
            inFlight.incrementAndGet();
        }
    }

    /**
     * @return 已认领但尚未处理完成（含重试等待中）的请求数
     */
    public int getInFlight() {
        return inFlight.get();
    }
}
