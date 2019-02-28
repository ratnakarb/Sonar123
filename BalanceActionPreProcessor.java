package com.ibm.iot.dep.events.balance;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.iot.dep.db.DBExecutor;
import com.ibm.iot.dep.db.ParameterValue;
import com.ibm.iot.dep.json.AbstractJSONActionPreProcessor;
import com.ibm.iot.dep.json.JSONObject;
import org.apache.kafka.common.metrics.Stat;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BalanceActionPreProcessor extends AbstractJSONActionPreProcessor {
    @Override
    public void process(JSONObject key, JSONObject value) {

        org.json.JSONObject preProcessObj = new org.json.JSONObject();
        Gson gson = new Gson();
        System.out.println("Balance PreProcessor Started : "+value.toString());

        if (value.hasProperty("d.BAL") && value.hasProperty("d.RC_ID")) {
            if ((value.getStringProperty("d.RC_ID").isEmpty())) {
                List<Map<String, Serializable>> getpurifierbydevice = getPurifierByDevice(value.getStringProperty("d.MID"));
                if (getpurifierbydevice.size() > 0) {
                    Iterator<Map<String, Serializable>> Igetpurifierbydevice = getpurifierbydevice.iterator();
                    while (Igetpurifierbydevice.hasNext()) {
                        Map<String, Serializable> purifierDetails = Igetpurifierbydevice.next();

                        List<Map<String, Serializable>> getConsumerByPurifier = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(), value.getStringProperty("d.MID"), "PENDING_RECHARGE");

                        if (getConsumerByPurifier.size() > 0) {
                            String rechargeStatus = null;

                            Iterator<Map<String, Serializable>> IgetConsumerByPurifier = getConsumerByPurifier.iterator();
                            while (IgetConsumerByPurifier.hasNext()) {
                                Map<String, Serializable> consumerDetails = IgetConsumerByPurifier.next();

                                preProcessObj.put("ConsumerId", consumerDetails.get("CONSUMER_ID"));
                                preProcessObj.put("RechargeId", consumerDetails.get("RECHARGE_ID"));
                                preProcessObj.put("LastConnectivity", consumerDetails.get("LAST_CONNECTIVITY"));
                                preProcessObj.put("CircuitId", consumerDetails.get("CCIRCUIT_ID"));
                                preProcessObj.put("PurifierId", consumerDetails.get("PURIFIER_ID"));
                                preProcessObj.put("RechargeStatus", consumerDetails.get("RECHARGE_STATUS"));
                                preProcessObj.put("RechargeGallons", consumerDetails.get("RECHARGE_GALLONS"));
                                preProcessObj.put("Mobile", consumerDetails.get("Mobile"));


                                if (consumerDetails.get("RECHARGE_STATUS") == null) {
                                    preProcessObj.put("RechargeStatusNull",consumerDetails.get("RECHARGE_STATUS").toString()); // This will be used to add previous state of Recharge
                                }

                                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                                String lConnTs = consumerDetails.get("LAST_CONNECTIVITY").toString();
                                String waterbal_ts = value.getStringProperty("d.TS");

                                LocalDateTime waterBaldt = LocalDateTime.parse(waterbal_ts, format);
                                LocalDateTime lConndt = LocalDateTime.parse(lConnTs, format);

                                if (ChronoUnit.MINUTES.between(waterBaldt, lConndt) > 5) {
                                    // Moving the payment status to reconcile
                                    preProcessObj.put("CalWaterBalance","True");
                                    preProcessObj.put("Reconcile", "True");
                                } else {
                                    System.out.println("WaterBalance : Time Difference is not more than 5 minutes");
                                }

                            }
                        } else {
                            // No Recharge which are pending
                            List<Map<String, Serializable>> getConsumerNopending = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(),
                                    value.getStringProperty("d.MID"), null);

                            if (getConsumerNopending.size() > 0) {
                                Iterator<Map<String, Serializable>> IgetConsumerNoPending = getConsumerNopending.iterator();
                                while (IgetConsumerNoPending.hasNext()) {
                                    Map<String, Serializable> getConsumerNoPend = IgetConsumerNoPending.next();

                                    preProcessObj.put("ConsumerId", getConsumerNoPend.get("CONSUMER_ID"));
                                    preProcessObj.put("CircuitId", getConsumerNoPend.get("CCIRCUIT_ID"));
                                    preProcessObj.put("PurifierId", getConsumerNoPend.get("PURIFIER_ID"));
                                    preProcessObj.put("RechargeGallons", getConsumerNoPend.get("RECHARGE_GALLONS"));
                                    preProcessObj.put("CalWaterBalance", "True"); // Calculate the water Balance.
                                }

                            } else {
                                // kind of replacement scenario
                                Map<String,Serializable> getPurifierNew = GetConsumerByPurifier_ForReplacement(purifierDetails.get("HUL_PURIFIER_ID").toString(),purifierDetails.get("IBOT_CCIRCUIT_ID").toString());
                                if(getPurifierNew != null){
                                    preProcessObj.put("ConsumerId", getPurifierNew.get("CONSUMER_ID"));
                                    preProcessObj.put("CircuitId", getPurifierNew.get("CCIRCUIT_ID"));
                                    preProcessObj.put("PurifierId", getPurifierNew.get("PURIFIER_ID"));
                                    preProcessObj.put("CalWaterBalance", "True");
                                }
                            }
                        }
                    }
                } else {
                    // No purifier related to the consumer
                    System.out.println("WaterBalance : No purifier is related to the device");
                }
            } else {
                // Hourly Balance with RCID
                List<Map<String, Serializable>> getPurifierConsumerRecords = getPurifierConsumerByDevice(value.getStringProperty("d.MID"));
                if (getPurifierConsumerRecords.size() > 0) {
                    Iterator<Map<String, Serializable>> IgetPurifierConsumerRecords = getPurifierConsumerRecords.iterator();
                    while (IgetPurifierConsumerRecords.hasNext()) {
                        Map<String, Serializable> getPurifierConsumer = IgetPurifierConsumerRecords.next();

                        preProcessObj.put("ConsumerId", getPurifierConsumer.get("CONSUMER_ID"));
                        preProcessObj.put("CircuitId", getPurifierConsumer.get("CCIRCUIT_ID"));
                        preProcessObj.put("PurifierId", getPurifierConsumer.get("PURIFIER_ID"));

                        List<Map<String, Serializable>> getPaymentDetails = getPaymentStatus((Integer) getPurifierConsumer.get("CONSUMER_ID"), value.getStringProperty("d.RC_ID"));
                        if (getPaymentDetails.size() > 0) {

                            Iterator<Map<String, Serializable>> IgetPaymentDetails = getPaymentDetails.iterator();
                            while (IgetPaymentDetails.hasNext()) {

                                Map<String, Serializable> getPaymentDetail = IgetPaymentDetails.next();
                                if ("PENDING_RECHARGE".equals(getPaymentDetail.get("RECHARGE_STATUS").toString())) {

                                    preProcessObj.put("RechargeStatus", getPaymentDetail.get("RECHARGE_STATUS"));
                                    preProcessObj.put("RechargeGallons", getPaymentDetail.get("RECHARGE_GALLONS"));
                                    preProcessObj.put("Mobile", getPaymentDetail.get("MOBILE"));
                                    preProcessObj.put("CalWaterBalance", "True");
                                    preProcessObj.put("RechargeStatus", "True"); // Update the status to Success and Send SMS Notification
                                    preProcessObj.put("Notification", "True");
                                } else {

                                    List<Map<String, Serializable>> getConsumerByPurifier = getConsumerByPurifier(getPurifierConsumer.get("PURIFIER_ID").toString(), value.getStringProperty("d.MID")
                                            , "PENDING_RECHARGE");

                                    if (getConsumerByPurifier.size() > 0) {
                                        String rechargeStatus = null;

                                        Iterator<Map<String, Serializable>> IgetConsumerByPurifier = getConsumerByPurifier.iterator();
                                        while (IgetConsumerByPurifier.hasNext()) {
                                            Map<String, Serializable> consumerDetails = IgetConsumerByPurifier.next();

                                            preProcessObj.put("ConsumerId", consumerDetails.get("CONSUMER_ID"));
                                            preProcessObj.put("RechargeId", consumerDetails.get("RECHARGE_ID"));
                                            preProcessObj.put("LastConnectivity", consumerDetails.get("LAST_CONNECTIVITY"));
                                            preProcessObj.put("CircuitId", consumerDetails.get("CCIRCUIT_ID"));
                                            preProcessObj.put("PurifierId", consumerDetails.get("PURIFIER_ID"));
                                            preProcessObj.put("RechargeStatus", consumerDetails.get("RECHARGE_STATUS"));
                                            preProcessObj.put("RechargeGallons", consumerDetails.get("RECHARGE_GALLONS"));
                                            preProcessObj.put("Mobile", consumerDetails.get("Mobile"));


                                            if (consumerDetails.get("RECHARGE_STATUS") == null) {
                                                preProcessObj.put("RechargeStatusNull",consumerDetails.get("RECHARGE_STATUS").toString()); // This will be used to add previous state of Recharge
                                            }

                                            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                                            String lConnTs = consumerDetails.get("LAST_CONNECTIVITY").toString();
                                            String waterbal_ts = value.getStringProperty("d.TS");

                                            LocalDateTime waterBaldt = LocalDateTime.parse(waterbal_ts, format);
                                            LocalDateTime lConndt = LocalDateTime.parse(lConnTs, format);

                                            if (ChronoUnit.MINUTES.between(waterBaldt, lConndt) > 5) {
                                                // Moving the payment status to reconcile
                                                preProcessObj.put("CalWaterBalance", "True");
                                                preProcessObj.put("Reconcile", "True");
                                            } else {
                                                System.out.println("WaterBalance : Time Difference is not more than 5 minutes");
                                            }

                                        }
                                    } else {
                                        // No Recharge which are pending
                                        List<Map<String, Serializable>> getConsumerNopending = getConsumerByPurifier(getPurifierConsumer.get("PURIFIER_ID").toString(),
                                                value.getStringProperty("d.MID"), null);

                                        if (getConsumerNopending.size() > 0) {
                                            Iterator<Map<String, Serializable>> IgetConsumerNoPending = getConsumerNopending.iterator();
                                            while (IgetConsumerNoPending.hasNext()) {
                                                Map<String, Serializable> getConsumerNoPend = IgetConsumerNoPending.next();

                                                preProcessObj.put("ConsumerId", getConsumerNoPend.get("CONSUMER_ID"));
                                                preProcessObj.put("CircuitId", getConsumerNoPend.get("CCIRCUIT_ID"));
                                                preProcessObj.put("PurifierId", getConsumerNoPend.get("PURIFIER_ID"));
                                                preProcessObj.put("RechargeGallons", getConsumerNoPend.get("RECHARGE_GALLONS"));
                                                preProcessObj.put("CalWaterBalance ", "True"); // Calculate the water Balance.
                                            }

                                        } else {
                                            // kind of replacement scenario
                                        }
                                    }

                                }
                            }
                        } else {
                            // Add the above first steps as function and call it.
                            List<Map<String, Serializable>> getpurifierbydevice = getPurifierByDevice(value.getStringProperty("d.MID"));
                            if (getpurifierbydevice.size() > 0) {
                                Iterator<Map<String, Serializable>> Igetpurifierbydevice = getpurifierbydevice.iterator();
                                while (Igetpurifierbydevice.hasNext()) {
                                    Map<String, Serializable> purifierDetails = Igetpurifierbydevice.next();

                                    List<Map<String, Serializable>> getConsumerByPurifier = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(), value.getStringProperty("d.MID")
                                            , "PENDING_RECHARGE");

                                    if (getConsumerByPurifier.size() > 0) {
                                        String rechargeStatus = null;

                                        Iterator<Map<String, Serializable>> IgetConsumerByPurifier = getConsumerByPurifier.iterator();
                                        while (IgetConsumerByPurifier.hasNext()) {
                                            Map<String, Serializable> consumerDetails = IgetConsumerByPurifier.next();

                                            preProcessObj.put("ConsumerId", consumerDetails.get("CONSUMER_ID"));
                                            preProcessObj.put("RechargeId", consumerDetails.get("RECHARGE_ID"));
                                            preProcessObj.put("LastConnectivity", consumerDetails.get("LAST_CONNECTIVITY"));
                                            preProcessObj.put("CircuitId", consumerDetails.get("CCIRCUIT_ID"));
                                            preProcessObj.put("PurifierId", consumerDetails.get("PURIFIER_ID"));
                                            preProcessObj.put("RechargeStatus", consumerDetails.get("RECHARGE_STATUS"));
                                            preProcessObj.put("RechargeGallons", consumerDetails.get("RECHARGE_GALLONS"));
                                            preProcessObj.put("Mobile", consumerDetails.get("Mobile"));


                                            if (consumerDetails.get("RECHARGE_STATUS") == null) {
                                                preProcessObj.put("RechargeStatusNull",consumerDetails.get("RECHARGE_STATUS").toString()); // This will be used to add previous state of Recharge
                                            }

                                            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                                            String lConnTs = consumerDetails.get("LAST_CONNECTIVITY").toString();
                                            String waterbal_ts = value.getStringProperty("d.TS");

                                            LocalDateTime waterBaldt = LocalDateTime.parse(waterbal_ts, format);
                                            LocalDateTime lConndt = LocalDateTime.parse(lConnTs, format);

                                            if (ChronoUnit.MINUTES.between(waterBaldt, lConndt) > 5) {
                                                // Moving the payment status to reconcile
                                                preProcessObj.put("CalWaterBalance", "True");
                                                preProcessObj.put("Reconcile", "True");
                                            } else {
                                                System.out.println("WaterBalance : Time Difference is not more than 5 minutes");
                                            }

                                        }
                                    } else {
                                        // No Recharge which are pending
                                        List<Map<String, Serializable>> getConsumerNopending = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(),
                                                value.getStringProperty("d.MID"), null);

                                        if (getConsumerNopending.size() > 0) {
                                            Iterator<Map<String, Serializable>> IgetConsumerNoPending = getConsumerNopending.iterator();
                                            while (IgetConsumerNoPending.hasNext()) {
                                                Map<String, Serializable> getConsumerNoPend = IgetConsumerNoPending.next();

                                                preProcessObj.put("ConsumerId", getConsumerNoPend.get("CONSUMER_ID"));
                                                preProcessObj.put("CircuitId", getConsumerNoPend.get("CCIRCUIT_ID"));
                                                preProcessObj.put("PurifierId", getConsumerNoPend.get("PURIFIER_ID"));
                                                preProcessObj.put("RechargeGallons", getConsumerNoPend.get("RECHARGE_GALLONS"));
                                                preProcessObj.put("CalWaterBalance ", "True"); // Calculate the water Balance.
                                            }

                                        } else {
                                            // kind of replacement scenario
                                        }
                                    }
                                }
                            } else {
                                // No purifier related to the consumer
                                System.out.println("WaterBalance : No purifier is related to the device");
                            }

                        }
                    }
                } else {
                    System.out.println("WaterBalance : No Active Record found in the system");
                }
            }

        } else {
            // Hourly Balance with out RCID
            List<Map<String, Serializable>> getpurifierbydevice = getPurifierByDevice(value.getStringProperty("d.MID"));
            if (getpurifierbydevice.size() > 0) {
                Iterator<Map<String, Serializable>> Igetpurifierbydevice = getpurifierbydevice.iterator();
                while (Igetpurifierbydevice.hasNext()) {
                    Map<String, Serializable> purifierDetails = Igetpurifierbydevice.next();

                    List<Map<String, Serializable>> getConsumerByPurifier = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(), value.getStringProperty("d.MID"), "PENDING_RECHARGE");

                    if (getConsumerByPurifier.size() > 0) {
                        String rechargeStatus = null;

                        Iterator<Map<String, Serializable>> IgetConsumerByPurifier = getConsumerByPurifier.iterator();
                        while (IgetConsumerByPurifier.hasNext()) {
                            Map<String, Serializable> consumerDetails = IgetConsumerByPurifier.next();

                            preProcessObj.put("ConsumerId", consumerDetails.get("CONSUMER_ID"));
                            preProcessObj.put("RechargeId", consumerDetails.get("RECHARGE_ID"));
                            preProcessObj.put("LastConnectivity", consumerDetails.get("LAST_CONNECTIVITY"));
                            preProcessObj.put("CircuitId", consumerDetails.get("CCIRCUIT_ID"));
                            preProcessObj.put("PurifierId", consumerDetails.get("PURIFIER_ID"));
                            preProcessObj.put("RechargeStatus", consumerDetails.get("RECHARGE_STATUS"));
                            preProcessObj.put("RechargeGallons", consumerDetails.get("RECHARGE_GALLONS"));
                            preProcessObj.put("Mobile", consumerDetails.get("Mobile"));


                            if (consumerDetails.get("RECHARGE_STATUS") == null) {
                                preProcessObj.put("RechargeStatusNull",consumerDetails.get("RECHARGE_STATUS").toString()); // This will be used to add previous state of Recharge
                            }

                            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                            String lConnTs = consumerDetails.get("LAST_CONNECTIVITY").toString();
                            String waterbal_ts = value.getStringProperty("d.TS");

                            LocalDateTime waterBaldt = LocalDateTime.parse(waterbal_ts, format);
                            LocalDateTime lConndt = LocalDateTime.parse(lConnTs, format);

                            if (ChronoUnit.MINUTES.between(waterBaldt, lConndt) > 5) {
                                // Moving the payment status to reconcile
                                preProcessObj.put("CalWaterBalance", "True");
                                preProcessObj.put("Reconcile", "True");
                            } else {
                                System.out.println("WaterBalance : Time Difference is not more than 5 minutes");
                            }

                        }
                    } else {
                        // No Recharge which are pending
                        List<Map<String, Serializable>> getConsumerNopending = getConsumerByPurifier(purifierDetails.get("PURIFIER_ID").toString(),
                                value.getStringProperty("d.MID"), null);

                        if (getConsumerNopending.size() > 0) {
                            Iterator<Map<String, Serializable>> IgetConsumerNoPending = getConsumerNopending.iterator();
                            while (IgetConsumerNoPending.hasNext()) {
                                Map<String, Serializable> getConsumerNoPend = IgetConsumerNoPending.next();

                                preProcessObj.put("ConsumerId", getConsumerNoPend.get("CONSUMER_ID"));
                                preProcessObj.put("CircuitId", getConsumerNoPend.get("CCIRCUIT_ID"));
                                preProcessObj.put("PurifierId", getConsumerNoPend.get("PURIFIER_ID"));
                                preProcessObj.put("RechargeGallons", getConsumerNoPend.get("RECHARGE_GALLONS"));
                                preProcessObj.put("CalWaterBalance", "True"); // Calculate the water Balance.
                            }

                        } else {
                            // kind of replacement scenario


                        }
                    }
                }
            } else {
                // No purifier related to the consumer
                System.out.println("WaterBalance : No purifier is related to the device");
            }
        }

        String json = gson.toJson(preProcessObj);
        System.out.println("Balance PreProcessor Completed : "+value.toString());
        context().put("BalancePreProcessor", json.getBytes());
    }

    /**
     * Get the purifier associated with the device. ( Balance even will come only after power on event, at that
     * moment it will change curcuit purifier status to active from active_pending)
     *
     * @param deviceId
     * @return
     */
    private static List<Map<String, Serializable>> getPurifierByDevice(String deviceId) {

        DBExecutor db = new DBExecutor();

        String sql = "SELECT CP.PURIFIER_ID AS PURIFIER_ID,PM.HUL_PURIFIER_ID as HUL_PURIFIER_ID,CM.CCIRCUIT_ID,CM.IBOT_CCIRCUIT_ID " +
                "FROM CCIRCUIT_MASTER CM,CCIRCUIT_PURIFIER_ASSOCIATION CP,PURIFIER_MASTER PM " +
                "WHERE CM.CCIRCUIT_ID=CP.CCIRCUIT_ID AND PM.PURIFIER_ID=CP.PURIFIER_ID " +
                "AND CM.IBOT_CCIRCUIT_ID= ? AND CM.FLAG='N' AND CM.STATUS='ACTIVE' " +
                "AND CP.FLAG='N' AND CP.STATUS='ACTIVE' AND PM.FLAG='N' AND PM.STATUS='ACTIVE'";


        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1, deviceId));

        return db.select(sql, parameterValues);
    }

    /**
     * Get consumer details by purifier
     *
     * @param hulPurifierId
     * @param deviceId
     * @return
     */
    public static List<Map<String, Serializable>> getConsumerByPurifier(String hulPurifierId, String deviceId, String status) {

        DBExecutor db = new DBExecutor();
        String sql = null;
        List<ParameterValue> parameterValues = new ArrayList<>();

        if (status != null) {
            sql = "select consumer_id, recharge_id, recharge_gallons, last_connectivity, ccircuit_id, recharge_status, purifier_id " +
                    "from (select rank() over ( order by substr(payment.recharge_id,4) desc) " +
                    "as rnk, cmaster.CONSUMER_ID as CONSUMER_ID, payment.RECHARGE_ID as RECHARGE_ID, " +
                    "payment.RECHARGE_GALLONS as RECHARGE_GALLONS, payment.LAST_CONNECTIVITY as LAST_CONNECTIVITY, " +
                    "payment.CCIRCUIT_ID as CCIRCUIT_ID, payment.RECHARGE_STATUS as RECHARGE_STATUS, payment.PURIFIER_ID as PURIFIER_ID " +
                    "from consumer_purifier_association cpa inner join purifier_master purifier on purifier.purifier_id = cpa.purifier_id " +
                    "inner join consumer_master cmaster on cmaster.consumer_id = cpa.consumer_id inner join payment_transaction payment " +
                    "on payment.consumer_id = cmaster.consumer_id inner join ccircuit_master circuit_master on circuit_master.ccircuit_id " +
                    "= payment.ccircuit_id where purifier.HUL_PURIFIER_ID= ? and circuit_master.IBOT_CCIRCUIT_ID= ? " +
                    "and payment.recharge_status= ? and cpa.flag = 'N' and cpa.status = 'ACTIVE' and purifier.flag = 'N' " +
                    "and purifier.status = 'ACTIVE' and cmaster.flag = 'N' and cmaster.status = 'ACTIVE' and circuit_master.flag = 'N' " +
                    "and circuit_master.status = 'ACTIVE' ) as ranked_table where ranked_table.rnk = 1";

            parameterValues.add(new ParameterValue(1, hulPurifierId));
            parameterValues.add(new ParameterValue(2, deviceId));
            parameterValues.add(new ParameterValue(3, status));
        } else {
            sql = "select consumer_id, recharge_id, recharge_gallons, last_connectivity, ccircuit_id, recharge_status, purifier_id " +
                    "from (select rank() over ( order by substr(payment.recharge_id,4) desc) as rnk, cmaster.CONSUMER_ID as CONSUMER_ID, " +
                    "payment.RECHARGE_ID as RECHARGE_ID, payment.RECHARGE_GALLONS as RECHARGE_GALLONS, payment.LAST_CONNECTIVITY as LAST_CONNECTIVITY, " +
                    "payment.CCIRCUIT_ID as CCIRCUIT_ID, payment.RECHARGE_STATUS as RECHARGE_STATUS, payment.PURIFIER_ID as PURIFIER_ID " +
                    "from consumer_purifier_association cpa inner join purifier_master purifier on purifier.purifier_id = cpa.purifier_id inner join " +
                    "consumer_master cmaster on cmaster.consumer_id = cpa.consumer_id inner join payment_transaction payment on " +
                    "payment.consumer_id = cmaster.consumer_id inner join ccircuit_master circuit_master on circuit_master.ccircuit_id = payment.ccircuit_id " +
                    "where purifier.HUL_PURIFIER_ID= ? and circuit_master.IBOT_CCIRCUIT_ID= ? " +
                    "and cpa.flag = 'N' and cpa.status = 'ACTIVE' and purifier.flag = 'N' and purifier.status = 'ACTIVE' and cmaster.flag = 'N' and " +
                    "cmaster.status = 'ACTIVE' and circuit_master.flag = 'N' and circuit_master.status = 'ACTIVE' ) as ranked_table where ranked_table.rnk = 1";

            parameterValues.add(new ParameterValue(1, hulPurifierId));
            parameterValues.add(new ParameterValue(2, deviceId));

        }
        return db.select(sql, parameterValues);
    }


    private static List<Map<String, Serializable>> getPurifierConsumerByDevice(String deviceId) {

        DBExecutor db = new DBExecutor();

        String sql = "SELECT DISTINCT CP.PURIFIER_ID AS PURIFIER_ID,PM.HUL_PURIFIER_ID as HUL_PURIFIER_ID,CM.CCIRCUIT_ID,COM.CONSUMER_ID," +
                "COM.CONSUMER_HUL_ID,COM.MOBILE FROM CCIRCUIT_MASTER CM, CCIRCUIT_PURIFIER_ASSOCIATION CP, PURIFIER_MASTER PM, " +
                "CONSUMER_PURIFIER_ASSOCIATION CPA, CONSUMER_MASTER COM WHERE CM.CCIRCUIT_ID=CP.CCIRCUIT_ID AND PM.PURIFIER_ID=CP.PURIFIER_ID " +
                "AND PM.PURIFIER_ID=CPA.PURIFIER_ID AND CPA.CONSUMER_ID=COM.CONSUMER_ID AND CM.IBOT_CCIRCUIT_ID= ? AND CM.FLAG='N' " +
                "AND CM.STATUS='ACTIVE' AND CP.FLAG='N' AND CP.STATUS='ACTIVE' AND PM.FLAG='N' AND PM.STATUS='ACTIVE' AND CPA.FLAG='N' AND CPA.STATUS='ACTIVE' " +
                "AND COM.FLAG='N' AND COM.STATUS='ACTIVE'";


        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1, deviceId));

        return db.select(sql, parameterValues);
    }

    // This function will only be used to when there is value for the RCID.

    /**
     * Get recharge status from consumerId and recharge Id.
     *
     * @param consmerId
     * @param rcId
     * @return
     */
    private static List<Map<String, Serializable>> getPaymentStatus(int consmerId, String rcId) {

        DBExecutor db = new DBExecutor();

        String sql = "SELECT PT.RECHARGE_GALLONS as RECHARGE_GALLONS,PT.RECHARGE_STATUS as RECHARGE_STATUS," +
                "PT.CCIRCUIT_ID as CCIRCUIT_ID,PT.CONSUMER_ID as CONSUMER_ID,PT.PURIFIER_ID as PURIFIER_ID,CM.MOBILE as MOBILE," +
                "PT.LAST_CONNECTIVITY as LCONN_TS  FROM PAYMENT_TRANSACTION PT,CONSUMER_MASTER CM WHERE PT.CONSUMER_ID=CM.CONSUMER_ID " +
                "AND PT.RECHARGE_ID= ? AND PT.CONSUMER_ID= ?";

        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1, rcId));
        parameterValues.add(new ParameterValue(2, consmerId));

        return db.select(sql, parameterValues);
    }


    private static Map<String,Serializable> GetConsumerByPurifier_ForReplacement(String hulpurifierid, String ibotcircuitid){
        DBExecutor db = new DBExecutor();

        String sql = "SELECT distinct  CCM.CCIRCUIT_ID as CCIRCUIT_ID,CON.CONSUMER_ID as " +
                "CONSUMER_ID,P.PURIFIER_ID as PURIFIER_ID FROM CONSUMER_PURIFIER_ASSOCIATION CP,PURIFIER_MASTER P," +
                "CONSUMER_MASTER CON, CCIRCUIT_MASTER CCM " +
                "WHERE P.PURIFIER_ID=CP.PURIFIER_ID AND CON.CONSUMER_ID=CP.CONSUMER_ID AND " +
                "P.HUL_PURIFIER_ID= ? AND CCM.IBOT_CCIRCUIT_ID= ? " +
                "AND P.FLAG='N' AND P.STATUS='ACTIVE' AND CCM.FLAG='N' AND CCM.STATUS='ACTIVE' " +
                "AND CP.STATUS='ACTIVE' AND CP.FLAG='N' AND CON.STATUS='ACTIVE' AND CON.FLAG='N'";


        List<ParameterValue> parameterValues = new ArrayList<>();
        parameterValues.add(new ParameterValue(1, hulpurifierid));
        parameterValues.add(new ParameterValue(2, ibotcircuitid));

        List<Map<String,Serializable>> list =  db.select(sql, parameterValues);

        if(list.size() > 0){
            return list.get(0);
        } else {
            return null;
        }

    }
}
