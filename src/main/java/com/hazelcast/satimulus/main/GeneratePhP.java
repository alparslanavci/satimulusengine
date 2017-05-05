package com.hazelcast.satimulus.main;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Processor;
import com.hazelcast.satimulus.domain.PhoneHome;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.hazelcast.jet.impl.util.Util.memoize;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.*;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public final class GeneratePhP extends AbstractProcessor{
    static final Pattern DELIMITER = Pattern.compile(";");

    public static final int MAX_LAG = 1000;
    private final long periodNanos;
    private long nextSchedule;
    private int order = 0;

    private final List<String> phList = new ArrayList<>();
    private final Supplier<String[]> phSupplier =
            memoize(() -> phList.stream().toArray(String[]::new));


    private GeneratePhP(long periodNanos) {
        setCooperative(false);
        this.periodNanos = periodNanos;
    }

    public static Distributed.Supplier<Processor> generatePh(final int phPerSecPerMember) {
        return () -> new GeneratePhP((long) (SECONDS.toNanos(1) / phPerSecPerMember));
    }

    @Override
    protected void init(Context context) {
        nextSchedule = System.nanoTime() + periodNanos;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        String initial = (String) item;
        phList.add(initial);
        return true;
    }

    @Override
    public boolean complete() {
        String[] phs = phSupplier.get();
        if (phs.length == 0) {
            return true;
        }
        long now = nanoTime();
        for (; now < nextSchedule; now = nanoTime()) {
            if (now < nextSchedule - MILLISECONDS.toNanos(1)) {
                parkNanos(MICROSECONDS.toNanos(100));
            }
            if (now < nextSchedule - MICROSECONDS.toNanos(50)) {
                parkNanos(1);
            }
        }
        for (; nextSchedule <= now; nextSchedule += periodNanos) {
            if (order >= phs.length)
                order = 0;
            String phoneHomeString = phs[order++];
            //TODO
            String[] strings = DELIMITER.split(phoneHomeString);
            PhoneHome item = new PhoneHome();
            item.setPingTime(Long.parseLong(strings[2]));
            item.setLicense(strings[5]);
            item.setOrg(strings[10]);
            emit(item);
        }
        return false;
    }


}
