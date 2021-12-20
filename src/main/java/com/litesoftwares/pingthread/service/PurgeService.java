package com.litesoftwares.pingthread.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Service
public class PurgeService {
    @Autowired
    private DatabaseService dbService;
    private Twitter twitter = TwitterFactory.getSingleton();

    //@Scheduled(cron="0 * * * * *")
  //  @Scheduled(fixedDelay = 1000)
    public void purgeDeletedThreads(){

        try {

            List<Map<String, Object>> threadInfo = dbService.fetchThreads();

            for (Map<String, Object> userInfo : threadInfo) {
            long threadId = (long) userInfo.get("thread_id");
                System.out.println(threadId + " itr");
            try {
                twitter.showStatus(threadId);
            } catch (TwitterException e){
                if (e.getMessage().contains("No status found with that ID")){
                    dbService.deleteThread(threadId);
                }
                if (e.getMessage().contains("rate limit")){
                    Thread.sleep(10000);
                }
            }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
       // System.out.println("Tokens updated: " + LocalDate.now());
    }
}
