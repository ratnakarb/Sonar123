package com.ibm.iot.dep.events.balance;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.ibm.iot.dep.events.testMain;
import com.ibm.iot.dep.json.AbstractJSONEventFilter;
import com.ibm.iot.dep.json.JSONObject;

import java.io.IOException;
import java.util.Scanner;

public class BalanceEventFilter extends AbstractJSONEventFilter {
    @Override
    public boolean filter(JSONObject key, JSONObject value) {
        if(value.hasProperty("d.BAL")) {
            System.out.println("Balance Event : true");
            return true;
        } else {
            System.out.println("Balance event : False");
            return false;
        }
    }
}
