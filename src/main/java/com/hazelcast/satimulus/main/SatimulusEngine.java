package com.hazelcast.satimulus.main;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.satimulus.filter.HighPardotScoreUse;
import com.hazelcast.satimulus.filter.NewCustomerUse;
import com.hazelcast.satimulus.filter.NewLicenseUse;
import com.hazelcast.satimulus.filter.OldCustomerUse;
import com.hazelcast.satimulus.slidingwindow.OneCompanyLargeUse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;

public class SatimulusEngine {
    public static int phPerSecPerMember = 1;
    private JetInstance jet;
    public static final String PHONEHOME_LIST_NAME = "phList";
    public static final String FILTERING_ALERT_LIST_NAME = "filterAlertList";
    public static final String WINDOWING_ALERT_LIST_NAME = "windowAlertList";

    public static void main(String[] args) throws Throwable {
        try {
            new SatimulusEngine().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));
        System.out.println("Creating Jet instance 1");
        jet = Jet.newJetInstance(cfg);
//        System.out.println("Creating Jet instance 2");
//        Jet.newJetInstance(cfg);
        loadData();
    }

    private void loadData() {
        IStreamList<String> phoneHomeList = jet.getList(PHONEHOME_LIST_NAME);
        List<String> lines = docLines("C:\\Users\\Alparslan\\workspace\\satimulusengine\\src\\main\\resources\\phoneHomes.csv").collect(Collectors.toList());
        phoneHomeList.addAll(lines);
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void go() throws ExecutionException, InterruptedException {
        setup();
        startNewLicenseUse();
        startOneCompanyLargeUse();
        startNewCustomerUse();
        startOldCustomerUse();
        startHighPardotUse();
    }

    private void startHighPardotUse() {
        Runnable runnable = () -> {
            try {
                Job job = jet.newJob(HighPardotScoreUse.createDag());
                job.execute().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }

    private void startOldCustomerUse() {
        Runnable runnable = () -> {
            try {
                Job job = jet.newJob(OldCustomerUse.createDag());
                job.execute().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }

    private void startNewCustomerUse() {
        Runnable runnable = () -> {
            try {
                Job job = jet.newJob(NewCustomerUse.createDag());
                job.execute().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }

    private void startOneCompanyLargeUse() {
        Runnable runnable = () -> {
            try {
                Job job = jet.newJob(OneCompanyLargeUse.createDag());
                job.execute().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }

    private void startNewLicenseUse() throws ExecutionException, InterruptedException {
        Runnable runnable = () -> {
            try {
                Job job = jet.newJob(NewLicenseUse.createDag());
                job.execute().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }
}
