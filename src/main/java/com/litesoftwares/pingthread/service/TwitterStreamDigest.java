package com.litesoftwares.pingthread.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import twitter4j.*;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Service
public class TwitterStreamDigest {
    private Twitter twitter = TwitterFactory.getSingleton();
    private TwitterStream twitterStream = new TwitterStreamFactory(twitter.getConfiguration()).getInstance();

    @Inject
    private ThreadService threadService;

    @Inject
    private ThreadPoolTaskExecutor taskExecutor;

    private BlockingQueue<Status> queue = new ArrayBlockingQueue<>(10000);
    @Value("${keyword.track}")
    private String keywordToTrack;

    @Value("${twitterProcessing.enabled}")
    private boolean twitterProcessing;

    private void run() {

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        FilterQuery fq = new FilterQuery();
        String keywords[] = {keywordToTrack};

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);
    }

    @PostConstruct
    public void afterPropertiesSet() {
        try {

            if(twitterProcessing) {
                for (int i = 0; i < taskExecutor.getMaxPoolSize(); i++) {
                    taskExecutor.execute(new TweetProcessor(threadService,queue));
                }
                run();
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
