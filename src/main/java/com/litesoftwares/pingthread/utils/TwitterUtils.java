package com.litesoftwares.pingthread.utils;

import org.apache.commons.text.StringEscapeUtils;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterUtils {



    public static String transformGifLinks(String text){
        String urlValidationRegex = "(.*/)*.+\\.(mp4|3gp|gif)$";
        Pattern p = Pattern.compile(urlValidationRegex);
        Matcher m = p.matcher(text);
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            String found =m.group(0);
            m.appendReplacement(sb, "<video class='gif' src='"+found+"' controls></video>");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static String transformVideoLinks(String text){
        String urlValidationRegex = "(.*/)*.+\\.(mp4|3gp|gif)$";
        Pattern p = Pattern.compile(urlValidationRegex);
        Matcher m = p.matcher(text);
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            String found =m.group(0);
            m.appendReplacement(sb, "<video class='vid' src='"+found+"' controls></video>");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static String transformImageLinks(String text){
        String urlValidationRegex = "(.*/)*.+\\.(png|jpg|jpeg|PNG|JPG|JPEG)$";
        Pattern p = Pattern.compile(urlValidationRegex);
        Matcher m = p.matcher(text);
        StringBuffer sb = new StringBuffer();
        while(m.find()){
            String found =m.group(0);
            m.appendReplacement(sb, "<img src='"+found+"'/>");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static String stripLinks(String textInput){
        String matchLinks = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        return  textInput.replaceAll(matchLinks, "");
    }

    public static String stripShortLinks(String textInput){
        String matchLinks = "(https?):(\\/\\/t\\.co\\/([A-Za-z0-9]|[A-Za-z]){10})";
        return  textInput.replaceAll(matchLinks,"");
    }

    public static String formatTweetEmbed(String tweetLink){
        return "<div class='quote'>" + tweetLink + "</div>";
    }

    public static int getMaxVariant(List<Integer> bitrate, int variant){
        bitrate.add(variant);
        Collections.sort(bitrate);
        return bitrate.get(bitrate.size()-1);
    }

    public static String cleanTweet(String text){
        return stripShortLinks(StringEscapeUtils.escapeHtml4(StringEscapeUtils.escapeJava(removeUnrolls(text))));
    }


    public long fetchTweetId(String mediaUrl){
        String[] text = mediaUrl.split("/");
        String[] statusID = text[text.length-1].split("[?]");
        return Long.parseLong(statusID[0]);
    }

    public static String removeUnrolls(String text) {

        return text.replaceAll("@threadreaderapp unroll","")
                .replaceAll("@ThreadReaderApp unroll","")
                .replaceAll("unroll @threadReaderApp","")
                .replaceAll("unroll @threadreaderapp","")
                .replaceAll("@threader_app compile","")
                .replaceAll("@Threader_App compile","")
                .replaceAll("compile @threader_app","");
                //.replaceAll("@pingthread unroll","")
               // .replaceAll("@PingThread unroll","")
                //.replaceAll("unroll @PingThread","");

    }
}