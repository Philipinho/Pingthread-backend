package com.litesoftwares.pingthread.controller;

import com.litesoftwares.pingthread.model.ThreadData;
import com.litesoftwares.pingthread.service.DatabaseService;
import com.litesoftwares.pingthread.service.HttpThreadService;
import com.litesoftwares.pingthread.service.RefreshService;
import com.litesoftwares.pingthread.service.ThreadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class ThreadController {
    @Autowired
    private ThreadService threadService;
    @Autowired
    private HttpThreadService httpThreadService;
    @Autowired
    private RefreshService refreshService;
    @Autowired
    private DatabaseService db;

   @GetMapping("/")
    public String receiver() {
        return "Why are you here?";
    }

    @RequestMapping(path = "api/thread/add", method = RequestMethod.POST)
    public @ResponseBody
    ThreadData addThread(@RequestParam long id) {
        return httpThreadService.processThread(id);
    }

    @RequestMapping(path = "api/thread/refresh", method = RequestMethod.POST)
    public @ResponseBody
    ThreadData refreshThread(@RequestParam long id) {
        return refreshService.httpRefreshThread(id);
    }

    @RequestMapping(path = "api/thread/exist", method = RequestMethod.POST)
    public @ResponseBody
    Boolean threadExist(@RequestParam long id) {
        return db.threadExist(id);
    }

}