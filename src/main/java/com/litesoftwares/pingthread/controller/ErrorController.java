package com.litesoftwares.pingthread.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

@RestController
public class ErrorController implements org.springframework.boot.web.servlet.error.ErrorController {

    @RequestMapping("/error")
    public Map<String,Object> handleError(HttpServletRequest request){
        Integer statusCode = (Integer)request.getAttribute("javax.servlet.error.status_code");
        Exception exception = (Exception) request.getAttribute("javax.servlet.error.exception");

        Map<String,Object> map = new HashMap<>();

        if (statusCode == 403){
            map.put("error_code", statusCode);
            map.put("message", "Sorry, a valid API key is required.");
        } else {
            map.put("error_code", statusCode);

            try {
                map.put("message", exception == null ? "an error occurred" : exception.getMessage().split(":")[1]);
            } catch (Exception e) {
                map.put("message", "an error occurred");
            }
        }

        return map;
    }

    @Override
    public String getErrorPath() {
        return "/error";
    }
}
