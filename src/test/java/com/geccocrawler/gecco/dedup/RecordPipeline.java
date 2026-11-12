package com.geccocrawler.gecco.dedup;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.geccocrawler.gecco.annotation.PipelineName;
import com.geccocrawler.gecco.pipeline.Pipeline;
import com.geccocrawler.gecco.request.HttpPostRequest;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.spider.SpiderBean;

/**
 * 记录所有成功处理（下载+渲染+管道）的请求身份，供回归断言使用。
 * 通过反射读取SpiderBean上的getRequest()，兼容测试包内所有bean。
 */
@PipelineName("dedupRecordPipeline")
public class RecordPipeline implements Pipeline<SpiderBean> {

    private static final List<String> PROCESSED = new CopyOnWriteArrayList<String>();

    @Override
    public void process(SpiderBean bean) {
        HttpRequest request = extractRequest(bean);
        if (request == null) {
            return;
        }
        String method = request instanceof HttpPostRequest ? "POST" : "GET";
        PROCESSED.add(method + " " + request.getUrl());
    }

    private HttpRequest extractRequest(SpiderBean bean) {
        try {
            Method getter = bean.getClass().getMethod("getRequest");
            Object value = getter.invoke(bean);
            return value instanceof HttpRequest ? (HttpRequest) value : null;
        } catch (Exception e) {
            return null;
        }
    }

    public static List<String> getProcessed() {
        return PROCESSED;
    }

    /**
     * 统计包含指定片段（通常是本次测试服务的base url/端口）的已处理请求，
     * 避免其它测试用例残留线程写入的全局记录干扰断言。
     */
    public static int countContaining(String urlFragment) {
        int count = 0;
        for (String entry : PROCESSED) {
            if (entry.contains(urlFragment)) {
                count++;
            }
        }
        return count;
    }

    public static void reset() {
        PROCESSED.clear();
    }
}
