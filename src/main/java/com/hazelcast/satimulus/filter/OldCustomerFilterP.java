package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.satimulus.domain.Alert;
import com.hazelcast.satimulus.domain.PhoneHome;

import java.util.Set;

public class OldCustomerFilterP extends AbstractProcessor{

    private Set<String> oldCustomers;

    @Override
    protected boolean tryProcess0(Object item) {
        oldCustomers = (Set<String>) item;
        return true;
    }

    @Override
    protected boolean tryProcess1(Object item) {

        PhoneHome phoneHome = (PhoneHome) item;
        return !oldCustomers.contains(phoneHome.getOrg())
                || tryEmit(new Alert(System.currentTimeMillis(),
                OldCustomerUse.ALERT_DESC,
                phoneHome));
    }

}
