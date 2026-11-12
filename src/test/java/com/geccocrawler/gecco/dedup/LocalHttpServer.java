package com.geccocrawler.gecco.dedup;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * 本地HTTP测试服务，不依赖外网。
 * 记录每个路径被实际请求的次数，并支持可编程的响应（普通页面、302跳转、POST回显、超时）。
 */
public class LocalHttpServer {

    private final HttpServer server;
    private final int port;
    private final Map<String, AtomicInteger> hitCount = new ConcurrentHashMap<String, AtomicInteger>();
    private final Map<String, Responder> responders = new ConcurrentHashMap<String, Responder>();
    private final Map<String, String> lastPostBody = new ConcurrentHashMap<String, String>();

    public interface Responder {
        void respond(HttpExchange exchange, String path) throws IOException;
    }

    public LocalHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String path = exchange.getRequestURI().getPath();
                hitCount.computeIfAbsent(identity(exchange), k -> new AtomicInteger(0)).incrementAndGet();
                if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    java.util.Scanner s = new java.util.Scanner(exchange.getRequestBody(), "UTF-8").useDelimiter("\\A");
                    lastPostBody.put(path, s.hasNext() ? s.next() : "");
                }
                Responder responder = responders.get(path);
                if (responder != null) {
                    responder.respond(exchange, path);
                    return;
                }
                defaultPage(exchange, path);
            }
        });
        server.start();
    }

    private static String identity(HttpExchange exchange) {
        return exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath();
    }

    public int getHitCount(String method, String path) {
        AtomicInteger counter = hitCount.get(method + " " + path);
        return counter == null ? 0 : counter.get();
    }

    public String getLastPostBody(String path) {
        return lastPostBody.get(path);
    }

    public void setResponder(String path, Responder responder) {
        responders.put(path, responder);
    }

    public void redirect(final String path, final String target) {
        responders.put(path, new Responder() {
            @Override
            public void respond(HttpExchange exchange, String p) throws IOException {
                exchange.getResponseHeaders().add("Location", target);
                exchange.sendResponseHeaders(302, -1);
                exchange.close();
            }
        });
    }

    public void slow(final String path, final long delayMs) {
        responders.put(path, new Responder() {
            @Override
            public void respond(HttpExchange exchange, String p) throws IOException {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                defaultPage(exchange, p);
            }
        });
    }

    public void html(final String path, final String body) {
        responders.put(path, new Responder() {
            @Override
            public void respond(HttpExchange exchange, String p) throws IOException {
                byte[] bytes = body.getBytes("UTF-8");
                exchange.getResponseHeaders().add("Content-Type", "text/html;charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                OutputStream os = exchange.getResponseBody();
                os.write(bytes);
                os.close();
            }
        });
    }

    private void defaultPage(HttpExchange exchange, String path) throws IOException {
        String body = "<html><head><title>" + path + "</title></head><body><h1>" + path
                + "</h1><a href=\"/diamond/a\">a</a><a href=\"/diamond/b\">b</a></body></html>";
        byte[] bytes = body.getBytes("UTF-8");
        exchange.getResponseHeaders().add("Content-Type", "text/html;charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(bytes);
        os.close();
    }

    public String url(String path) {
        return "http://127.0.0.1:" + port + path;
    }

    public Map<String, Integer> snapshot() {
        Map<String, Integer> snapshot = new HashMap<String, Integer>();
        for (Map.Entry<String, AtomicInteger> entry : hitCount.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public void stop() {
        server.stop(0);
    }
}
