package com.hazelcast.satimulus;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.satimulus.domain.Alert;

import java.util.Date;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("jet").setPassword("jet-pass");
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        IList<Alert> alertList = hz.getList("filterAlertList");
        while (true){
            for (Alert alert : alertList) {
                String org = alert.getPhoneHome().getOrg();
                System.out.println(new Date(alert.getTimestamp()) + ": " + org + " -> " + alert.getDesc());
            }
            System.out.println("#################################");
            Thread.sleep(5000);
        }
    }
}
