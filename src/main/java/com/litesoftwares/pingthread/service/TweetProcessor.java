package com.litesoftwares.pingthread.service;

import twitter4j.Status;

import java.util.concurrent.BlockingQueue;

public class TweetProcessor implements Runnable {

    private final BlockingQueue<Status> queue;
    private ThreadService threadService;

////    @Scheduled(cron = "0 0 0/12 * * *") //Refresh every 12 hours
    public TweetProcessor(ThreadService threadService, BlockingQueue<Status> queue) {
        this.threadService = threadService;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Status tweet = queue.take();
                processTweet(tweet);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void processTweet(Status tweet) {
        try {
            System.out.println(tweet.getId());
            threadService.processMentions(tweet);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("hello");
        }
    }
}
