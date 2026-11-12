package com.geccocrawler.gecco.dedup;

import java.util.List;

import com.geccocrawler.gecco.annotation.Gecco;
import com.geccocrawler.gecco.annotation.Href;
import com.geccocrawler.gecco.annotation.HtmlField;
import com.geccocrawler.gecco.annotation.Request;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.spider.HtmlBean;

/**
 * 菱形页面：两个不同页面(/diamond/a、/diamond/b)各自派生指向同一个下游地址的请求。
 * 两个页面可能被不同Spider线程处理，派生请求进入各自的线程私有队列，
 * 用于验证身份边界跨私有队列的统一去重。
 */
@Gecco(matchUrl = {
        "http://127.0.0.1:{port}/diamond/a",
        "http://127.0.0.1:{port}/diamond/b"
}, pipelines = "dedupRecordPipeline", timeout = 2000)
public class DiamondPage implements HtmlBean {

    private static final long serialVersionUID = 1L;

    @Request
    private HttpRequest request;

    @Href(click = true)
    @HtmlField(cssPath = "a.leaf")
    private List<String> leafLinks;

    public HttpRequest getRequest() {
        return request;
    }

    public void setRequest(HttpRequest request) {
        this.request = request;
    }

    public List<String> getLeafLinks() {
        return leafLinks;
    }

    public void setLeafLinks(List<String> leafLinks) {
        this.leafLinks = leafLinks;
    }
}
