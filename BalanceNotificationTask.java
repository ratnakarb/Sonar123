package com.ibm.iot.dep.events.balance;

import com.ibm.iot.dep.json.AbstractJSONNotificationTask;
import com.ibm.iot.dep.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BalanceNotificationTask extends AbstractJSONNotificationTask {
    @Override
    public void execute(JSONObject key, JSONObject value) {
        //send water balance app event
        byte[] preProcessorValue = (byte[]) context().get("BalancePreProcessor");
        JSONObject object = new JSONObject(preProcessorValue);

        System.out.println("Balance Notifications Task Started : "+value.toString());

        if (object.hasProperty("map.RechargeStatus")) {
            // Invoking Notification Gateway API
            System.out.println("Notification Send to the consumer");

        }

        System.out.println("Balance Notification Task Completed : "+value.toString());
        context().put("BalancenotificationKey", preProcessorValue);
    }


    @Override
    public String getState() {
        return "balance-notification";
    }
}
