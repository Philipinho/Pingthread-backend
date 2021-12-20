package com.litesoftwares.pingthread.service;

import com.litesoftwares.pingthread.model.ThreadData;
import com.litesoftwares.pingthread.utils.TwitterUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Status;
import twitter4j.URLEntity;

import java.util.*;
import java.util.stream.Collectors;

import static com.litesoftwares.pingthread.service.ThreadService.formatThread;
import static com.litesoftwares.pingthread.service.ThreadService.getTweetSinceId;
import static com.litesoftwares.pingthread.utils.TwitterUtils.*;
import static com.litesoftwares.pingthread.utils.TwitterUtils.cleanTweet;
import static com.litesoftwares.pingthread.utils.TwitterUtils.transformImageLinks;

@Service
public class RefreshService {
    @Autowired
    private DatabaseService dbService;

    public ThreadData httpRefreshThread(long tweetId){

        ThreadData threadData = new ThreadData();
        threadData.setType("refresh");

        try {

            if (tweetId != -1) {

                List<Status> tweetSince = getTweetSinceId(tweetId);
                List<Status> formatTweets = formatThread(tweetSince);

                if (formatTweets.size() >= 2){

                    StringBuilder mergeContent = new StringBuilder();

                    List<String> hashtagList = new ArrayList<>();

                    Map<String, String> linksInTweet = new HashMap<>();

                    for (Status status : formatTweets) {
                        String tweetText = status.getText();

                        URLEntity[] urlEntities = status.getURLEntities();

                        if (urlEntities.length == 0) {
                            tweetText = TwitterUtils.stripLinks(tweetText);
                        } else {

                            for (URLEntity entity : urlEntities) {
                                linksInTweet.put(entity.getURL(), entity.getExpandedURL());
                            }

                            for (String shortLink : linksInTweet.keySet()) {
                                String expandedLink = linksInTweet.get(shortLink);

                                if (expandedLink.startsWith("https://twitter.com/") && expandedLink.contains("/status/")) {
                                    tweetText = tweetText.replaceAll(shortLink, "");
                                } else {
                                    tweetText = tweetText.replaceAll(shortLink, expandedLink);
                                }
                            }
                        }

                        mergeContent.append("<div class='thread-part'>\n").append("<p class='tweet-text'>\n")
                                .append(cleanTweet(tweetText.
                                        replaceAll("\n\n\n", "\n")).replaceAll("\n", "\n<br>"))
                                .append("</p>");

                        mergeContent.append("\n<div class='tweet-id'>").append(status.getId()).append("</div>");

                        // Traverse Quote Tweet Links
                        for (URLEntity url : urlEntities) {
                            if (url.getExpandedURL().startsWith("https://twitter.com/") && (url.getExpandedURL().contains("/status/") || url.getExpandedURL().contains("/i/"))) {
                                mergeContent.append("\n").append(formatTweetEmbed(url.getExpandedURL()));
                            }
                        }

                        HashtagEntity[] hashtagEntities = status.getHashtagEntities();

                        for (HashtagEntity hashtag : hashtagEntities) {
                            hashtagList.add(hashtag.getText().toLowerCase());
                        }

                        //Traverse media files
                        MediaEntity[] mediaEntities = status.getMediaEntities();

                        for (MediaEntity media : mediaEntities) {

                            if (media.getType().equalsIgnoreCase("video")) {
                                String videoUrl = "";
                                MediaEntity.Variant[] variants = media.getVideoVariants();

                                List<Integer> bitrate = new ArrayList<>();

                                for (MediaEntity.Variant variant : variants) {
                                    if (variant.getContentType().equalsIgnoreCase("video/mp4")) {
                                        if (variant.getBitrate() == getMaxVariant(bitrate, variant.getBitrate())) {
                                            videoUrl = variant.getUrl().split("[?]")[0];
                                        }
                                    }
                                }

                                mergeContent.append("\n").append(transformVideoLinks(videoUrl));

                            } else if(media.getType().equalsIgnoreCase("animated_gif")){

                                String gifURL = "";
                                MediaEntity.Variant[] variants = media.getVideoVariants();

                                List<Integer> bitrate = new ArrayList<>();

                                for (MediaEntity.Variant variant : variants) {
                                    if (variant.getContentType().equalsIgnoreCase("video/mp4")) {
                                        if (variant.getBitrate() == getMaxVariant(bitrate, variant.getBitrate())) {
                                            gifURL = variant.getUrl().split("[?]")[0];
                                        }
                                    }
                                }

                                mergeContent.append("\n").append(transformGifLinks(gifURL));
                            }

                            else if (media.getType().equalsIgnoreCase("photo")){
                                mergeContent.append("\n").append(transformImageLinks(media.getMediaURLHttps()));
                            }
                        }

                        mergeContent.append("\n</div>\n\n");
                    }

                    List<String> hashtags = hashtagList.stream().distinct().collect(Collectors.toList());

                    long authorUserId = formatTweets.get(0).getUser().getId();
                    String username = formatTweets.get(0).getUser().getScreenName();
                    String name = StringEscapeUtils.escapeHtml4(formatTweets.get(0).getUser().getName());
                    String bio = formatTweets.get(0).getUser().getDescription();
                    String profilePic = formatTweets.get(0).getUser().get400x400ProfileImageURLHttps();
                    int verified = 0;
                    if (formatTweets.get(0).getUser().isVerified()) verified = 1;

                    long threadId = formatTweets.get(0).getId();
                    String threadSnippet = cleanTweet(formatTweets.get(0).getText());
                    String threadText = mergeContent.toString();
                    int threadCount = formatTweets.size();
                    String threadHashtags = hashtags.toString().replace("[","").replaceAll("]","");
                   // Date threadDate = formatTweets.get(0).getCreatedAt();

                    threadData.setId(threadId);

                    if (dbService.threadExist(threadId) && threadCount > dbService.getThreadCount(threadId)) {

                        dbService.updateThread(threadId, threadText, threadCount, threadHashtags, new Date());

                        if (dbService.authorExist(authorUserId)){
                            dbService.updateAuthor(authorUserId,username,name,bio,profilePic,verified,new Date());
                        } else {
                            dbService.saveAuthor(authorUserId, username, name, bio, profilePic,verified);
                        }

                        threadData.setMessage("Thread refresh successful");
                        threadData.setStatus(true);
                        threadData.setCode(1);

                    }  else {
                        threadData.setMessage("Thread is up to date");
                        threadData.setStatus(false);
                        threadData.setCode(3);
                    }

                } else {
                    threadData.setId(formatTweets.get(0).getId());
                    threadData.setMessage("Thread has less than 2 Tweets");
                    threadData.setStatus(false);
                    threadData.setCode(2);
                }
            }
        } catch (Exception e) {
            threadData.setId(tweetId);
            threadData.setMessage("Unable to update thread.");
            threadData.setStatus(false);
            threadData.setCode(0);
            e.printStackTrace();
        }

        return threadData;
    }
}
