package com.geccocrawler.gecco.dedup;

import com.geccocrawler.gecco.annotation.Gecco;
import com.geccocrawler.gecco.annotation.HtmlField;
import com.geccocrawler.gecco.annotation.Request;
import com.geccocrawler.gecco.annotation.Text;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.spider.HtmlBean;

/**
 * 匹配本地测试服务各测试路径的通用SpiderBean，走默认httpClientDownloader。
 * 显式列出路径以避免与{@link DiamondPage}的/diamond匹配冲突。
 */
@Gecco(matchUrl = {
        "http://127.0.0.1:{port}/seed",
        "http://127.0.0.1:{port}/shared",
        "http://127.0.0.1:{port}/post",
        "http://127.0.0.1:{port}/redirect/from",
        "http://127.0.0.1:{port}/redirect/target",
        "http://127.0.0.1:{port}/isolate",
        "http://127.0.0.1:{port}/custom",
        "http://127.0.0.1:{port}/jmx",
        "http://127.0.0.1:{port}/loop",
        "http://127.0.0.1:{port}/diamond/leaf"
}, pipelines = "dedupRecordPipeline", timeout = 2000)
public class DedupPage implements HtmlBean {

    private static final long serialVersionUID = 1L;

    @Request
    private HttpRequest request;

    @Text
    @HtmlField(cssPath = "h1")
    private String title;

    public HttpRequest getRequest() {
        return request;
    }

    public void setRequest(HttpRequest request) {
        this.request = request;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
