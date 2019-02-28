package com.ibm.iot.dep.events.balance;

import com.ibm.iot.dep.db.DBExecutor;
import com.ibm.iot.dep.db.ParameterValue;
import com.ibm.iot.dep.json.AbstractJSONDatabaseActionTask;
import com.ibm.iot.dep.json.JSONObject;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class BalanceDatabaseTask extends AbstractJSONDatabaseActionTask {
    @Override
    public void execute(JSONObject key, JSONObject value) {
        byte[] preProcessorValue = (byte[]) context().get("BalancePreProcessor");
        JSONObject object = new JSONObject(preProcessorValue);
        System.out.println("Balance Database Task started : "+value.toString());

        if(object.hasProperty("map.RechargeStatus")){
            updatePaymentStatus(value.getStringProperty("d.RC_ID"),"SUCCESS");
            insertPaymentAudit(value.getStringProperty("d.RC_ID"),object.getStringProperty("map.ConsumerId"),"SUCCESS","PENDING_RECHARGE");
        }

        if(object.hasProperty("map.Reconcile")){
            updatePaymentStatus(value.getStringProperty("d.RC_ID"),"PENDING_RECONCILE");
            insertPaymentAudit(value.getStringProperty("d.RC_ID"),object.getStringProperty("map.ConsumerId"),"PENDING_RECONCILE",object.getStringProperty("map.RechargeStatusNull"));
        }

        if(object.hasProperty("map.CalWaterBalance")){
            // Calculate Water balance.
            insertWaterConsumption(object.getIntegerProperty("map.ConsumerId"),object.getIntegerProperty("map.PurifierId"),object.getIntegerProperty("map.CircuitId"),Double.parseDouble(value.getStringProperty("d.BAL")),0,value.getStringProperty("d.TS"));
        }

        System.out.println("Balance Database Task completed : "+value.toString());
        context().put("BalanceDataBaseKey",preProcessorValue);
    }

    @Override
    public String getState() {
        return "Balance-database";
    }

    private static void updatePaymentStatus(String rcId, String status){
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DBExecutor db = new DBExecutor();

        String sql = "UPDATE PAYMENT_TRANSACTION SET RECHARGE_STATUS= ? ,MODIFIED_BY='EventProcessor',MODIFIED_DATE= ? WHERE RECHARGE_ID= ? AND RECHARGE_STATUS='PENDING_RECHARGE'";

        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1,status));
        parameterValues.add(new ParameterValue(2, LocalDateTime.now().format(format)));
        parameterValues.add(new ParameterValue(3,rcId));

        db.beginTrans();
        db.upsert(sql,parameterValues);
        db.endTrans();
    }

    private static void insertPaymentAudit(String rcId, String consumerId, String status, String prevStatus){
        DBExecutor db = new DBExecutor();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String sql = "INSERT INTO PAYMENT_TRANSACTION_AUDIT(RECHARGE_ID,CONSUMER_ID," +
                "PREV_RECHARGE_STATUS,CURRENT_RECHARGE_STATUS,RECHARGE_TS," +
                "CREATED_BY,CREATED_DATE) VALUES( ? , ? , ? , ? , ?, 'ADMIN', ?)";

        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1,rcId));
        parameterValues.add(new ParameterValue(2,consumerId));
        parameterValues.add(new ParameterValue(3,status));
        parameterValues.add(new ParameterValue(4,prevStatus));
        parameterValues.add(new ParameterValue(5,LocalDateTime.now().format(format)));
        parameterValues.add(new ParameterValue(6,LocalDateTime.now().format(format)));

        db.beginTrans();
        db.upsert(sql,parameterValues);
        db.endTrans();
    }

    private static void insertWaterConsumption(int consumerId, int purifierId, int circuitId, double currBalance,double curr_recharge,String eventTs){
        DBExecutor db = new DBExecutor();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String sql = "INSERT INTO WATER_CONSUMPTION(CONSUMER_ID,PURIFIER_ID,CCIRCUIT_ID,BASE,CURR_BALANCE,PREV_BALANCE,CURR_RECHARGE,NET_RECHARGE,CURR_CONSUMPTION,NET_CONSUMPTION,PNET_CONSUMPTION,CNET_CONSUMPTION,LAST_RECH_ID,LAST_RECH_TS,EVENT_TS) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Timestamp ts = Timestamp.valueOf(LocalDateTime.parse(eventTs,format));

        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1,consumerId));
        parameterValues.add(new ParameterValue(2,purifierId));
        parameterValues.add(new ParameterValue(3,circuitId));
        parameterValues.add(new ParameterValue(4,0));
        parameterValues.add(new ParameterValue(5,currBalance));
        parameterValues.add(new ParameterValue(6,0));
        parameterValues.add(new ParameterValue(7,curr_recharge));
        parameterValues.add(new ParameterValue(8,0));
        parameterValues.add(new ParameterValue(9,0));
        parameterValues.add(new ParameterValue(10,0));
        parameterValues.add(new ParameterValue(11,0));
        parameterValues.add(new ParameterValue(12,0));
        parameterValues.add(new ParameterValue(13,"eventprocesor"));
        parameterValues.add(new ParameterValue(14,LocalDateTime.now().format(format)));
        parameterValues.add(new ParameterValue(15,ts));

        db.beginTrans();
        db.upsert(sql,parameterValues);
        db.endTrans();
    }
}
