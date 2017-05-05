package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.satimulus.domain.Alert;
import com.hazelcast.satimulus.domain.PhoneHome;

import java.util.Set;

public class NewCustomerFilterP extends AbstractProcessor{

    private Set<String> existingCustomers;

    @Override
    protected boolean tryProcess0(Object item) {
        existingCustomers = (Set<String>) item;
        return true;
    }

    @Override
    protected boolean tryProcess1(Object item) {

        PhoneHome phoneHome = (PhoneHome) item;
        return existingCustomers.contains(phoneHome.getOrg())
                || tryEmit(new Alert(System.currentTimeMillis(),
                NewCustomerUse.ALERT_DESC,
                phoneHome));
    }

}
