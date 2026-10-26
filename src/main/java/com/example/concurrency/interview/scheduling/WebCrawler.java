package com.example.concurrency.interview.scheduling;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

interface HtmlParser {
    List<String> getUrls(String url);   // given: slow network call
}

public class WebCrawler {
    private final HtmlParser parser;
    private final String hostname;
    private final Queue<String> toVisit = new ArrayDeque<>();   // found, not fetched yet
    private final Set<String> seen = new HashSet<>();          // ever queued: never fetch twice
    private int running = 0;                                   // pages being fetched right now

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    public WebCrawler(String startUrl, HtmlParser parser) {
        this.parser = parser;
        this.hostname = hostOf(startUrl);
        seen.add(startUrl);
        toVisit.add(startUrl);
    }

    public List<String> crawl(int numWorkers) throws InterruptedException {
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();                  // wait until the crawl is finished
        }
        return new ArrayList<>(seen);       // safe without lock: all workers joined
    }

    private void workerLoop() {
        while (true) {
            String url;
            try {
                url = take();
            } catch (InterruptedException e) {
                return;
            }
            if (url == null) return;        // crawl finished

            List<String> found = new ArrayList<>();
            try {
                found = parser.getUrls(url);    // slow: OUTSIDE the lock
            } catch (RuntimeException e) {
                // broken page: keep the empty list
            }
            finish(found);                  // always called exactly once
        }
    }

    private String take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            // nothing queued, but someone is fetching and may find more: wait
            while (toVisit.isEmpty() && running > 0) {
                changed.await();
            }
            if (toVisit.isEmpty()) return null;     // empty AND nobody fetching: done
            running++;
            return toVisit.poll();
        } finally {
            lock.unlock();
        }
    }

    private void finish(List<String> found) {
        lock.lock();
        try {
            running--;
            for (String u : found) {
                String host = hostOf(u);
                if (host == null) continue;                     // bad link: skip
                if (host.equals(hostname) && seen.add(u)) {     // same host, first time
                    toVisit.add(u);
                }
            }
            changed.signalAll();            // new URLs, or maybe the crawl is done
        } finally {
            lock.unlock();
        }
    }

    // "http://news.yahoo.com/news" → "news.yahoo.com"; bad link → null
    private static String hostOf(String url) {
        String[] parts = url.split("/");
        return parts.length < 3 ? null : parts[2];
    }

    public static void main(String[] args) throws InterruptedException {
        Map<String, List<String>> web = new HashMap<>();
        web.put("http://site.com/a", List.of("http://site.com/b", "http://site.com/c", "http://other.com/x"));
        web.put("http://site.com/b", List.of("http://site.com/a", "http://site.com/d", "abc"));
        web.put("http://site.com/c", List.of("http://site.com/d", "http://site.com/broken"));
        web.put("http://site.com/d", List.of("http://site.com/a"));

        HtmlParser parser = url -> {
            try { Thread.sleep(50); } catch (InterruptedException e) { }     // fake network delay
            if (url.endsWith("/broken")) throw new RuntimeException("404");  // a broken page
            return web.getOrDefault(url, List.of());
        };

        List<String> result = new WebCrawler("http://site.com/a", parser).crawl(4);
        Collections.sort(result);
        System.out.println(result);
        // [http://site.com/a, http://site.com/b, http://site.com/broken, http://site.com/c, http://site.com/d]
    }
}
