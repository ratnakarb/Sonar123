package com.ibm.iot.dep.events.balance;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.ibm.iot.dep.json.AbstractJSONSchemaValidator;
import com.ibm.iot.dep.json.JSONObject;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class BalanceSchemaValidator extends AbstractJSONSchemaValidator {
    @Override
    public boolean valid(JSONObject key, JSONObject value) {
        // TODO If fails insert into the activity master
        try {
            if(JSONDataValidation(value)){
                System.out.println("Balance Validation True");
                return true;
            } else {
                System.out.println("Balance Validation False -- Inserting to the DB");
                return false;
            }
        } catch (ProcessingException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean JSONDataValidation(JSONObject data) throws ProcessingException {
        int counter = 0;
        try {
            if(data.validJson("/Schemas/BAL.json")){

                try {
                    Double.parseDouble(data.getStringProperty("d.BAL"));
                } catch (NumberFormatException ex){
                    counter += 1;
                    System.out.println(ex.getMessage());
                }

                try {
                    String.valueOf(data.getStringProperty("d.MID"));
                } catch (NullPointerException ex){
                    counter += 1;
                    System.out.println(ex.getMessage());
                }

                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                try {
                    LocalDateTime.parse(data.getStringProperty("d.TS"), format);
                } catch (DateTimeParseException ex){
                    counter += 1;
                    System.out.println(ex.getMessage());
                }

                try {
                    if(!data.getStringProperty("d.RC_TS").isEmpty()){
                        LocalDateTime.parse(data.getStringProperty("d.RC_TS"), format);
                    }
                } catch (DateTimeParseException ex){
                    counter += 1;
                    System.out.println(ex.getMessage());
                }
            } else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return (counter==0);
    }
}
