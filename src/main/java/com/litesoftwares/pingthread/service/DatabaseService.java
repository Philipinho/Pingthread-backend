package com.litesoftwares.pingthread.service;

import com.litesoftwares.model.Tables;
import com.litesoftwares.model.tables.records.AuthorsRecord;
import com.litesoftwares.model.tables.records.ThreadsRecord;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class DatabaseService {
    @Autowired
    private DSLContext dslContext;

    @Autowired
    public DatabaseService(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public boolean authorExist(long authorUserId) {
        int code = dslContext.selectFrom(Tables.AUTHORS)
                .where(Tables.AUTHORS.USER_ID.eq(authorUserId)).execute();
        return code == 1;
    }

    public boolean isRestricted(long authorUserId) {
        int code = dslContext.selectFrom(Tables.RESTRICTED)
                .where(Tables.RESTRICTED.AUTHOR_USER_ID.eq(authorUserId)).execute();
        return code == 1;
    }

    public boolean threadExist(long threadId) {
        int code = dslContext.selectFrom(Tables.THREADS)
                .where(Tables.THREADS.THREAD_ID.eq(threadId)).execute();
        return code == 1;
    }

    public int getThreadCount(long threadId) {
        int threadCount = 0;

        try {
            threadCount = dslContext.select(Tables.THREADS.THREAD_COUNT).from(Tables.THREADS)
                    .where(Tables.THREADS.THREAD_ID.eq(threadId)).fetchOne(Tables.THREADS.THREAD_COUNT);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
        return threadCount;
    }

    public String getThreadSnippet(long threadId) {
        String snippet = "";

        try {
            snippet = dslContext.select(Tables.THREADS.THREAD_SNIPPET).from(Tables.THREADS)
                    .where(Tables.THREADS.THREAD_ID.eq(threadId)).fetchOne(Tables.THREADS.THREAD_SNIPPET);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
        return snippet;
    }

    public void saveThread(long userId, long threadId, String threadSnippet, String threadText, int threadCount,
                           String hashtags,String lang, Date threadCreated) {
        try {

            ThreadsRecord record = new ThreadsRecord();
            record.setAuthorUserId(userId);
            record.setThreadId(threadId);
            record.setThreadSnippet(threadSnippet);
            record.setThreadText(threadText);
            record.setThreadCount(threadCount);
            record.setHashtags(hashtags);
            record.setThreadLang(lang);
            record.setTweetCreated(new Timestamp(threadCreated.getTime()));

            record.attach(this.dslContext.configuration());
            record.store();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

    public void saveAuthor(long userId, String username, String name, String bio, String profilePic,int verified) {
        try {
            AuthorsRecord record = new AuthorsRecord();
            record.setUserId(userId);
            record.setUsername(username);
            record.setName(name);
            record.setBio(bio);
            record.setProfilePicture(profilePic);
            record.setVerified(verified);

            record.attach(this.dslContext.configuration());
            record.store();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

    public void updateAuthor(long userId, String username, String name, String bio, String profilePic, int verified, Date authorUpdated) {
        try {
            dslContext.update(Tables.AUTHORS).set(Tables.AUTHORS.USERNAME, username)
                    .set(Tables.AUTHORS.NAME, name).set(Tables.AUTHORS.BIO, bio)
                    .set(Tables.AUTHORS.PROFILE_PICTURE, profilePic).set(Tables.AUTHORS.VERIFIED, verified)
                    .set(Tables.AUTHORS.AUTHOR_UPDATED,new Timestamp(authorUpdated.getTime()))
                    .where(Tables.AUTHORS.USER_ID.eq(userId)).execute();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }
    public void updateThread(long threadId, String threadText, int threadCount, String hashtags, Date threadUpdated) {
        try {
            dslContext.update(Tables.THREADS).set(Tables.THREADS.THREAD_TEXT,threadText)
                    .set(Tables.THREADS.THREAD_COUNT,threadCount).set(Tables.THREADS.HASHTAGS,hashtags)
                    .set(Tables.THREADS.THREAD_UPDATED,new Timestamp(threadUpdated.getTime()))
                    .where(Tables.THREADS.THREAD_ID.eq(threadId)).execute();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }

    //for deletion
    public List<Map<String, Object>> fetchThreads(){
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> threads =
                null;
        try {
            threads = this.dslContext
                    .select(Tables.THREADS.THREAD_ID)
                    .from(Tables.THREADS).fetchMaps();
                          //  .and(Tables.USERS.ACTIVE.eq(activeStatusCode))).fetchMaps();
        } catch (DataAccessException e) {
            e.printStackTrace();
        }

        return threads;
    }

    public void deleteThread(long threadId){
        try {
            dslContext.deleteFrom(Tables.THREADS).where(Tables.THREADS.THREAD_ID.eq(threadId)).execute();
            System.out.println(threadId + " deleted");
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
    }
}