package com.geccocrawler.gecco.dedup;

import com.geccocrawler.gecco.annotation.Gecco;
import com.geccocrawler.gecco.annotation.HtmlField;
import com.geccocrawler.gecco.annotation.Request;
import com.geccocrawler.gecco.annotation.Text;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.spider.HtmlBean;

/**
 * 使用可注入失败的flakyDownloader，验证可重试失败的重排。
 */
@Gecco(matchUrl = "http://127.0.0.1:{port}/flaky", pipelines = "dedupRecordPipeline",
        downloader = "flakyDownloader", timeout = 2000)
public class FlakyPage implements HtmlBean {

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
