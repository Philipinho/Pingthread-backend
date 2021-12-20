package com.litesoftwares.pingthread.service;

import com.litesoftwares.pingthread.utils.TwitterUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import twitter4j.*;

import java.util.*;
import java.util.stream.Collectors;

import static com.litesoftwares.pingthread.utils.TwitterUtils.*;

@Service
public class ThreadService {

    private static Twitter twitter = TwitterFactory.getSingleton();

    @Value("${website.url}")
    private String websiteUrl;

    @Autowired
    private DatabaseService dbService;

    public void processMentions(Status tweet){

        try {

            long tweetReferencedId = -1;
            long mentionTweetId = tweet.getId();
            String whoMentionedMe = tweet.getUser().getScreenName();

            if (tweet.getQuotedStatus() != null){
                tweetReferencedId = tweet.getQuotedStatusId();

            } else if (tweet.getInReplyToStatusId() != -1){
                tweetReferencedId = tweet.getInReplyToStatusId();
            }

            if ((tweet.getText().toLowerCase().contains("unroll") || tweet.getText().toLowerCase().contains("compile")) && tweetReferencedId != -1) {
                Status authorTweet = null;

                try {
                    authorTweet = twitter.showStatus(tweetReferencedId);
                } catch (TwitterException e) {
                    //We have been blocked by the author of this tweet
                    if (e.getMessage().contains("You have been blocked")){
                        String blockedMessage = "Sorry, this author has blocked us from viewing their tweets.";
                        replyTweet("@" + tweet.getUser().getScreenName() + " " + blockedMessage, mentionTweetId);
                        return;
                    }

                }

                if (dbService.isRestricted(authorTweet.getUser().getId())) {
                    String restrictionMessage = "Sorry, this author has restricted us from unrolling their threads.";
                    replyTweet("@" + tweet.getUser().getScreenName() + " " + restrictionMessage, mentionTweetId);

                } else {

                    if (dbService.threadExist(tweetReferencedId)) {
                        String threadLink = websiteUrl + "/thread/" + tweetReferencedId;

                        String threadSnippet = dbService.getThreadSnippet(tweetReferencedId);

                        tweetMessage(whoMentionedMe, authorTweet.getUser().getScreenName(), threadSnippet,threadLink, mentionTweetId);
                    } else {

                        List<Status> tweetSince = getTweetSinceId(tweetReferencedId);
                        List<Status> formatTweets = formatThread(tweetSince);

                        if (formatTweets.size() >= 2) {

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

                                    } else if (media.getType().equalsIgnoreCase("animated_gif")) {

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
                                    } else if (media.getType().equalsIgnoreCase("photo")) {
                                        mergeContent.append("\n").append(transformImageLinks(media.getMediaURLHttps()));
                                    }
                                }

                                mergeContent.append("\n</div>\n\n");
                            }

                            List<String> hashtags = hashtagList.stream().distinct().collect(Collectors.toList());

                            long authorUserId = formatTweets.get(0).getUser().getId();
                            String authorUsername = formatTweets.get(0).getUser().getScreenName();
                            String authorName = StringEscapeUtils.escapeHtml4(formatTweets.get(0).getUser().getName());
                            String authorBio = formatTweets.get(0).getUser().getDescription();
                            String authorPhoto = formatTweets.get(0).getUser().get400x400ProfileImageURLHttps();

                            int verified = 0;
                            if (formatTweets.get(0).getUser().isVerified()) verified = 1;

                            long threadId = formatTweets.get(0).getId();
                            String threadSnippet = cleanTweet(formatTweets.get(0).getText());
                            String threadText = mergeContent.toString();
                            int threadCount = formatTweets.size();
                            String threadHashtags = hashtags.toString().replace("[", "").replaceAll("]", "");
                            String threadLang = formatTweets.get(0).getLang();
                            Date threadDate = formatTweets.get(0).getCreatedAt();

                            String threadLink = websiteUrl + "/thread/" + threadId;

                            if (!dbService.threadExist(threadId)) { // important in case not the first tweet the bot was mentioned on
                                dbService.saveThread(authorUserId, threadId, threadSnippet, threadText, threadCount, threadHashtags, threadLang, threadDate);

                                if (dbService.authorExist(authorUserId)) {
                                    dbService.updateAuthor(authorUserId, authorUsername, authorName, authorBio, authorPhoto, verified, new Date());
                                } else {
                                    dbService.saveAuthor(authorUserId, authorUsername, authorName, authorBio, authorPhoto, verified);
                                }

                                tweetMessage(whoMentionedMe, authorUsername, threadSnippet,threadLink, mentionTweetId);

                            } else {
                                tweetMessage(whoMentionedMe, authorUsername, threadSnippet, threadLink, mentionTweetId);
                            }

                        }
                    }
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    public static List<Status> formatThread(List<Status> statuses) {
        List<Status> statusList = new ArrayList<>();
        statusList.add(statuses.get(0));

        List<Status> filtered = new ArrayList<>();
        long replyIdForNextTweet = statusList.get(0).getId();

        List<Status> upperTweets = readUpperTweets(replyIdForNextTweet, statuses.get(0).getUser().getScreenName());

        for (int i = 1; i < statuses.size(); i++) {
            if (statuses.get(i).getInReplyToStatusId() == replyIdForNextTweet) {
                replyIdForNextTweet = statuses.get(i).getId();
                filtered.add(statuses.get(i));
            }
        }

        filtered.addAll(0, statusList);
        if (upperTweets != null && upperTweets.size() > 0) {
            filtered.addAll(0, upperTweets);
        }

        return filtered;
    }

    public static List<Status> getTweetSinceId(long tweetId) {

        Status status = null;
        try {
            status = twitter.showStatus(tweetId);
        } catch (TwitterException e) {
            e.printStackTrace();

            if (e.getMessage().contains("Rate limit exceeded")){
                System.out.println("TweetSince: Status API Rate Limited. Sleeping for 5 minutes." );
                try {
                    Thread.sleep(300000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

        Paging paging = new Paging();
        paging.setSinceId(tweetId);
        paging.setCount(200);

        List<Status> tweets = new ArrayList<>();

        while (true) {

            List<Status> timeline = null;
            try {
                timeline = twitter.getUserTimeline(status.getUser().getId(), paging);
            } catch (TwitterException e) {
                e.printStackTrace();

                if (e.getMessage().contains("Rate limit exceeded")){
                    System.out.println("User Timeline API Rate Limited. Sleeping for 5 minutes." );
                    try {
                        Thread.sleep(300000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }

            Collections.sort(timeline, new Comparator<Status>() {
                @Override
                public int compare(Status status1, Status status2) {
                    return String.valueOf(status1.getId()).compareTo(String.valueOf(status2.getId()));
                }
            });

            boolean duplicate = false;

            for (Status stat : timeline) {
                if (stat.getId() == paging.getMaxId()) {
                    duplicate = true;
                }
            }

            if (duplicate) {
                timeline.remove(timeline.size() - 1);
            }

            if (timeline.size() == 0) break;

            paging.setMaxId(timeline.get(0).getId());

            tweets.addAll(0, timeline);
        }

        tweets.add(0, status);

        return tweets;
    }

    public static List<Status> readUpperTweets(long lastId, String screenName) {

        Status status = null;
        try {
            status = twitter.showStatus(lastId);
        } catch (TwitterException e) {

            if (e.getMessage().contains("Rate limit exceeded")){
                System.out.println("ReadUpperTweets: Status API Rate Limited. Sleeping for 5 minutes." );
                try {
                    Thread.sleep(300000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } else if (e.getMessage().contains("The URI requested is invalid or the resource requested")){
                System.out.println("del tweet ");
            } else {
                e.printStackTrace();
            }
        }

        long id = 0L;
        List<Status> tweets = new ArrayList<>();

//        tweets.add(status);

        if (status.getInReplyToStatusId() != -1) {
            do {
                id = status.getInReplyToStatusId();
                if (id != -1) {
                    try {
                        status = twitter.showStatus(id);
                    } catch (TwitterException e) {
                        if (e.getMessage().contains("you are not authorized to see this status")) {
                            System.out.println("Protected status: " + id);
                        }
                        else if (e.getMessage().contains("The URI requested is invalid or the resource requested")){
                            System.out.println("deleted tweet " + id);
                        } else {
                            System.out.println("exception tweet: " + id);
                            e.printStackTrace();

                        }
                        break;
                    }
                }

                if (status.getUser().getScreenName().equalsIgnoreCase(screenName)) {
                    tweets.add(status);
                }
            } while (status.getInReplyToStatusId() != -1
                    && status.getUser().getScreenName().equalsIgnoreCase(screenName));
        }

        Collections.reverse(tweets);

        return tweets;
    }

    private static void tweetMessage(String mentionUsername, String threadAuthor, String snippet, String threadLink, long mentionStatusId){
        String tweetSnippet = StringEscapeUtils.unescapeHtml4(stripLinks(snippet).substring(0, Math.min(100, snippet.length()))).replaceAll("\\R", " ") + "... ";

        String userMention = "@" + mentionUsername;

        String[] messages = {"Hi, thread unroll complete: @" + threadAuthor + ": " + tweetSnippet + threadLink + " ..",
                "Hey, here is your unroll: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Have a good read.",
                "Hey, thread is ready: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Enjoy your read.",
                "Gotcha: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Have a good read.",
                "Hi, thread compiled: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Enjoy your read.",
                "Hi, thread unrolled: @" + threadAuthor + ": " + tweetSnippet + threadLink + " enjoy your read.",
                "Hey, thread is ready: @" + threadAuthor + ": " + tweetSnippet + threadLink + " ..",
                "Hi, at your service: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Have a good read.",
                "Hey, thread unroll complete: @" + threadAuthor + ": " + tweetSnippet + threadLink + " Time to read."};

        int rand = (int)(messages.length * Math.random());

        String  messageText = userMention + " " + messages[rand];
        replyTweet(messageText,mentionStatusId);
    }

    private static void replyTweet(String text, long id) {

        try {
            Status status = twitter.updateStatus(
                    new StatusUpdate(text)
                            .inReplyToStatusId(id));

            System.out.println("Replied to: " + status.getInReplyToScreenName());

        } catch (TwitterException e) {
            System.out.println(e.getMessage());
        }
    }

}