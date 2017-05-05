package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.satimulus.domain.Alert;
import com.hazelcast.satimulus.domain.PhoneHome;

import java.util.Set;

public class HighPardotFilterP extends AbstractProcessor{

    private Set<String> highPardotList;

    @Override
    protected boolean tryProcess0(Object item) {
        highPardotList = (Set<String>) item;
        return true;
    }

    @Override
    protected boolean tryProcess1(Object item) {

        PhoneHome phoneHome = (PhoneHome) item;
        return !highPardotList.contains(phoneHome.getOrg())
                || tryEmit(new Alert(System.currentTimeMillis(),
                HighPardotScoreUse.ALERT_DESC,
                phoneHome));
    }

}
