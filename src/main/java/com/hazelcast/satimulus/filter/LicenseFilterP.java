package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.satimulus.domain.Alert;
import com.hazelcast.satimulus.domain.PhoneHome;

import java.util.Set;

public class LicenseFilterP extends AbstractProcessor{

    private Set<String> licenses;

    @Override
    protected boolean tryProcess0(Object item) {
        licenses = (Set<String>) item;
        return true;
    }

    @Override
    protected boolean tryProcess1(Object item) {

        PhoneHome phoneHome = (PhoneHome) item;
        return !licenses.contains(phoneHome.getLicense())
                || tryEmit(new Alert(System.currentTimeMillis(),
                NewLicenseUse.ALERT_DESC,
                phoneHome));
    }

}
