package com.ibm.iot.dep.events.balance;

import com.ibm.iot.dep.base.*;

public class BalanceEventProcessor extends AbstractEventProcessor<byte[],byte[]> {
    @Override
    public EventFilter<byte[], byte[]> getFilter() {
        return new BalanceEventFilter();
    }

    @Override
    public SchemaValidator<byte[], byte[]> getValid() {
        return new BalanceSchemaValidator();
    }

    @Override
    public ActionPreProcessor<byte[], byte[]> getActionPreProcessor() {
        return new BalanceActionPreProcessor();
    }

    @Override
    public ActionActivity<byte[], byte[]> getActionActivity() {
        return new BalanceActionActivity();
    }

    @Override
    public String getSource() {
        return "EVENT"; // Assuming this is for the event type ( Data, Event .. )
    }

    @Override
    public String getName() {
        return "balance";
    }
}
