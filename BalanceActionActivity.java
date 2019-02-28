package com.ibm.iot.dep.events.balance;

import com.ibm.iot.dep.base.AbstractActionActivity;
import com.ibm.iot.dep.base.ActionTask;
import com.ibm.iot.dep.events.common.WaterBalAppEvent;

import java.util.ArrayList;
import java.util.List;

public class BalanceActionActivity extends AbstractActionActivity<byte[],byte[]> {

    @Override
    public ActionTask<byte[], byte[]> getStart() {
        return new BalanceDatabaseTask();
    }

    @Override
    public List<ActionTask<byte[], byte[]>> getIntermediate() {
        List<ActionTask<byte[],byte[]>> list = new ArrayList<ActionTask<byte[],byte[]>>();
        list.add(new WaterBalAppEvent());
        return list;
    }

    @Override
    public ActionTask<byte[], byte[]> getEnd() {
        return new BalanceNotificationTask();
    }

}
