package com.geccocrawler.gecco.dedup;

import java.util.concurrent.atomic.AtomicInteger;

import com.geccocrawler.gecco.annotation.Downloader;
import com.geccocrawler.gecco.downloader.DownloadException;
import com.geccocrawler.gecco.downloader.DownloadTimeoutException;
import com.geccocrawler.gecco.downloader.HttpClientDownloader;
import com.geccocrawler.gecco.request.HttpRequest;
import com.geccocrawler.gecco.response.HttpResponse;

/**
 * 前failuresBeforeSuccess次下载抛出超时异常（可重试失败），之后委托给真实下载器。
 * 用于验证失败请求会释放/重排认领而不是被永久标记完成。
 */
@Downloader("flakyDownloader")
public class FlakyDownloader extends HttpClientDownloader {

    public static volatile int failuresBeforeSuccess = 2;

    private static final AtomicInteger ATTEMPTS = new AtomicInteger(0);

    public static int getAttempts() {
        return ATTEMPTS.get();
    }

    public static void reset(int failures) {
        failuresBeforeSuccess = failures;
        ATTEMPTS.set(0);
    }

    @Override
    public HttpResponse download(HttpRequest request, int timeout) throws DownloadException {
        int attempt = ATTEMPTS.incrementAndGet();
        if (attempt <= failuresBeforeSuccess) {
            throw new DownloadTimeoutException("simulated timeout attempt=" + attempt);
        }
        return super.download(request, timeout);
    }
}
