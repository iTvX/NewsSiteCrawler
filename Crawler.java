/**
 * Prerequisite: JDK 11
 * <p>
 * This class represents a web crawler that crawls a news website and collects statistics on the visited pages.
 * The crawler uses version 4.4 of the crawler4j library and requires importing the 23 jar files specified in
 * the homework assignment.
 * The crawler makes use of various JDK 11 features such as var, Optional, and stream API.
 * The crawler visits pages on the domain foxnews.com and ignores certain file types specified in the EXCLUSIONS pattern.
 * The crawler collects various statistics on each visited page such as download size, number of outgoing links, and content type.
 * The statistics are collected in the pageStats class and saved to three CSV files and one text file at the end of crawling.
 * <p>
 * The main method starts the crawling process and collects the local data of each crawler instance.
 * The local data is then reduced into a single pageStats object and used to save the crawling statistics to files.
 * <p>
 * Summary of the code:
 * The Crawler class extends the WebCrawler class and overrides several methods to implement the desired behavior.
 * The shouldVisit method filters URLs based on the EXCLUSIONS pattern and the newsSiteDomain variable.
 * The handlePageStatusCode and visit methods collect statistics on visited pages and store them in the pageStats object.
 * The pageStats class defines three ArrayLists to store the statistics collected by the Crawler class.
 * The UrlStatus, UrlInfo, and UrlDetail classes define the structure of the statistics stored in the ArrayLists.
 * The Main class starts the crawling process and saves the collected statistics to files.
 * <p>
 * Developed by Zijie Yu on 3/7/2023.
 */


import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Crawler extends WebCrawler {
    pageStats pageStats;
    String newsSiteDomain = "foxnews.com";
    private final static Pattern EXCLUSIONS = Pattern.compile(".*(\\.(vcf|js|avi|mp3|mid|css|mp4|xml|mov|mpeg|zip|ram|wav|m4v|gz))$");

    public Crawler() {
        pageStats = new pageStats();
    }

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String urlName = url.getURL().replaceAll("/$", "").replaceAll("[,]", "_").toLowerCase().replaceAll("^https?://(www\\.)?", "");
        pageStats.addUrlDetail(url.getURL(), urlName.startsWith(newsSiteDomain) ? "OK" : "N_OK");
        return !EXCLUSIONS.matcher(urlName).matches() && urlName.startsWith(newsSiteDomain);
    }


    @Override
    public void handlePageStatusCode(WebURL webUrl, int httpStatusCode, String statusDesc) {
        pageStats.addUrlStatus(webUrl.getURL(), httpStatusCode);
    }

    @Override
    public void visit(Page page) {
        var url = page.getWebURL().getURL();
        var contentType = Optional.ofNullable(page.getContentType())
                .map(c -> c.toLowerCase().split(";")[0])
                .orElse("");
        var linksSize = Optional.of(page.getParseData())
                .filter(HtmlParseData.class::isInstance)
                .map(HtmlParseData.class::cast)
                .map(HtmlParseData::getOutgoingUrls)
                .map(Set::size)
                .orElse(0);

        pageStats.addUrlInfo(url, page.getContentData().length, linksSize, contentType);
    }

    @Override
    public Object getMyLocalData() {
        return pageStats;
    }
}


class pageStats {
    ArrayList<UrlStatus> status;
    ArrayList<UrlInfo> info;
    ArrayList<UrlDetail> detail;

    public pageStats() {
        status = new ArrayList<UrlStatus>();
        info = new ArrayList<UrlInfo>();
        detail = new ArrayList<UrlDetail>();
    }

    public void addUrlStatus(String link, int code) {
        this.status.add(new UrlStatus(link, code));
    }

    public void addUrlInfo(String link, int fileSize, int badLink, String type) {
        this.info.add(new UrlInfo(link, fileSize, badLink, type));
    }

    public void addUrlDetail(String link, String indicator) {
        this.detail.add(new UrlDetail(link, indicator));
    }
}


class UrlStatus {
    String url;
    int num;

    public UrlStatus(String url, int num) {
        this.url = url;
        this.num = num;
    }

    public static int getNum(UrlStatus urlStatus) {
        return urlStatus.num;
    }

    public static String getUrl(UrlStatus urlStatus) {
        return urlStatus.url;
    }
}

class UrlInfo {
    String url;
    int size;
    int badLinks;
    String type;

    public UrlInfo(String url, int size, int badLinks, String type) {
        this.url = url;
        this.size = size;
        this.badLinks = badLinks;
        this.type = type;
    }

    public static String getUrl(UrlInfo urlInfo) {
        return urlInfo.url;
    }

    public static int getSize(UrlInfo urlInfo) {
        return urlInfo.size;
    }

    public static int getBadLinks(UrlInfo urlInfo) {
        return urlInfo.badLinks;
    }

    public static String getType(UrlInfo urlInfo) {
        return urlInfo.type;
    }

}

class UrlDetail {
    String url;
    String urlType;

    public UrlDetail(String url, String urlType) {
        this.url = url;
        this.urlType = urlType;
    }

    public static String getUrl(UrlDetail urlDetail) {
        return urlDetail.url;
    }

}

class Main {
    static int numCrawlers = 80;
    static String siteName = "foxnews";
    static pageStats stats;


    private static void saveCrawlingDataToCsv() throws Exception {
        Path filePath;
        filePath = Path.of("fetch_" + siteName + ".csv");
        Files.deleteIfExists(filePath);
        Files.writeString(filePath, "Fetched URL,Status Code\n");
        Path finalFilePath = filePath;
        stats.status.stream()
                .map(urlStatus -> urlStatus.url + "," + urlStatus.num + "\n")
                .forEach(line -> {
                    try {
                        Files.writeString(finalFilePath, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });


        filePath = Path.of("visit_" + siteName + ".csv");
        Files.deleteIfExists(filePath);
        Files.writeString(filePath, "Downloaded URL,Size in Bytes,No of outlinks,ContentType\n");
        Path finalFilePath2 = filePath;
        stats.info.stream()
                .map(urlInfo -> urlInfo.url + "," + urlInfo.size + "," + urlInfo.badLinks + "," + urlInfo.type + "\n")
                .forEach(line -> {
                    try {
                        Files.writeString(finalFilePath2, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });


        filePath = Path.of("urls_" + siteName + ".csv");
        Files.deleteIfExists(filePath);
        Files.writeString(filePath, "URL,Residence Indicator\n");
        Path finalFilePath1 = filePath;
        stats.detail.stream()
                .map(urlDetail -> urlDetail.url + "," + urlDetail.urlType + "\n")
                .forEach(line -> {
                    try {
                        Files.writeString(finalFilePath1, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }


    private static void saveCrawlStatsToFile() throws Exception {

        int numFetches = stats.status.size();
        Map<Integer, Long> codeCounts = stats.status.stream()
                .collect(Collectors.groupingBy(UrlStatus::getNum, Collectors.counting()));
        int successfulFetchCount = codeCounts.getOrDefault(200, 0L).intValue();
        int abortedOrFailedFetchesCount = numFetches - successfulFetchCount;
        int totalUrls = stats.detail.size();
        Set<String> uniqueUrls = stats.detail.stream()
                .filter(urlDetail -> "OK".equals(urlDetail.urlType))
                .map(UrlDetail::getUrl)
                .collect(Collectors.toSet());
        int uniqueUrlCount = uniqueUrls.size();
        int numUniqueUrls = uniqueUrls.size();
        int uniqueUrlsOutRes = totalUrls - uniqueUrlCount;

        AtomicInteger oneK = new AtomicInteger();
        AtomicInteger tenK = new AtomicInteger();
        AtomicInteger hundredK = new AtomicInteger();
        AtomicInteger oneM = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        HashMap<String, Integer> contentTypes = new HashMap<String, Integer>();

        stats.info.forEach(urlInfo -> {
            if (urlInfo.size < 1024) {
                oneK.getAndIncrement();
            } else if (urlInfo.size < 10240) {
                tenK.getAndIncrement();
            } else if (urlInfo.size < 102400) {
                hundredK.getAndIncrement();
            } else if (urlInfo.size < 1024 * 1024) {
                oneM.getAndIncrement();
            } else {
                other.getAndIncrement();
            }

            contentTypes.compute(urlInfo.type, (k, v) -> v == null ? 1 : v + 1);
        });
        try {
            var fileName = String.format("CrawlReport_%s.txt", siteName);
            var content = String.format("Name: Zijie Yu\nUSC ID: 8552094404\nNews site crawled: %s.com\nNumber of threads: %d\n\n" +
                            "Fetch Statistics\n================\n# fetches attempted: %d\n# fetches succeeded: %d\n# fetches failed or aborted: %d\n\n" +
                            "Outgoing URLs:\n==============\nTotal URLs extracted: %d\n# unique URLs extracted: %d\n# unique URLs within News Site: %d\n# unique URLs outside News Site: %d\n\n" +
                            "Status Codes:\n=============\n200 OK: %d\n301 Moved Permanently: %d\n302 Found: %d\n400 Bad Request Response: %d\n401 Unauthorized: %d\n403 Forbidden: %d\n404 Not Found: %d\n410 Gone: %d\n\n" +
                            "File Sizes:\n===========\n< 1KB: %d\n1KB ~ <10KB: %d\n10KB ~ <100KB: %d\n100KB ~ <1MB: %d\n>= 1MB: %d\n\n" +
                            "Content Types:\n==============\n%s",
                    siteName, numCrawlers, numFetches, successfulFetchCount, abortedOrFailedFetchesCount, totalUrls, numUniqueUrls, uniqueUrlCount, uniqueUrlsOutRes,
                    codeCounts.get(200), codeCounts.get(301), codeCounts.get(302), codeCounts.get(400), codeCounts.get(401), codeCounts.get(403), codeCounts.get(404), codeCounts.get(410),
                    oneK.intValue(), tenK.intValue(), hundredK.intValue(), oneM.intValue(), other.intValue(), contentTypes.entrySet().stream()
                            .map(e -> e.getKey() + ": " + e.getValue())
                            .collect(Collectors.joining("\n")));
            Files.writeString(Path.of(fileName), content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        codeCounts.forEach((key, value) -> System.out.println(key + " " + value));

    }


    private static List<Object> startCrawling() throws Exception {
        var config = new CrawlConfig();
        config.setCrawlStorageFolder("/Users/cyril/IdeaProjects/HW2/output");
        config.setMaxPagesToFetch(20000);
        config.setMaxDepthOfCrawling(16);
        config.setPolitenessDelay(50);
        config.setIncludeBinaryContentInCrawling(true);

        var pageFetcher = new PageFetcher(config);
        var robotstxtServer = new RobotstxtServer(new RobotstxtConfig(), pageFetcher);
        var controller = new CrawlController(config, pageFetcher, robotstxtServer);
        controller.addSeed("https://www.foxnews.com/");

        controller.start(Crawler.class, numCrawlers);
        return controller.getCrawlersLocalData();
    }

    public static void main(String[] args) throws Exception {
        stats = startCrawling().stream()
                .map(pageStats.class::cast)
                .reduce(new pageStats(), (result, data) -> {
                    result.status.addAll(data.status);
                    result.info.addAll(data.info);
                    result.detail.addAll(data.detail);
                    return result;
                });

        saveCrawlingDataToCsv();
        saveCrawlStatsToFile();
    }

}
