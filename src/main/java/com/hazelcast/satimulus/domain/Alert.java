package com.hazelcast.satimulus.domain;

import java.io.Serializable;

public class Alert implements Serializable{
    private static final long serialVersionUID = 1L;

    private long timestamp;
    private String desc;
    private PhoneHome phoneHome;

    public Alert(long timestamp, String desc, PhoneHome phoneHome) {
        this.timestamp = timestamp;
        this.desc = desc;
        this.phoneHome = phoneHome;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public PhoneHome getPhoneHome() {
        return phoneHome;
    }

    public void setPhoneHome(PhoneHome phoneHome) {
        this.phoneHome = phoneHome;
    }
}
