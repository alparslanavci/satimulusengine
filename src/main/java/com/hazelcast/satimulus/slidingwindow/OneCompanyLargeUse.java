package com.hazelcast.satimulus.slidingwindow;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.PunctuationPolicies;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.satimulus.domain.Alert;
import com.hazelcast.satimulus.domain.PhoneHome;
import com.hazelcast.satimulus.main.GeneratePhP;
import com.hazelcast.satimulus.main.SatimulusEngine;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowingProcessors.*;
import static com.hazelcast.satimulus.main.SatimulusEngine.phPerSecPerMember;

public class OneCompanyLargeUse {
    private static final String ALERT_DESC = "Large number of nodes in the same organization!";
    private static final int THRESHOLD = 2;

    public static final int MAX_LAG = 5;

    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 10;
    private static final int SLIDE_STEP_MILLIS = 2;

    public static DAG createDag() {
        DAG dag = new DAG();
        WindowDefinition windowDef = slidingWindowDef(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        Vertex phSource = dag.newVertex("ph-source", Processors.readList(SatimulusEngine.PHONEHOME_LIST_NAME));
        Vertex phGenerate = dag.newVertex("generate-ph", GeneratePhP.generatePh(phPerSecPerMember));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(PhoneHome::getPingTime, () -> PunctuationPolicies.cappingEventSeqLagAndRetention(MAX_LAG, 1000)
                        .throttleByFrame(windowDef)));
        Vertex groupByFrame = dag.newVertex("group-by-frame",
                groupByFrame(PhoneHome::getOrg, PhoneHome::getPingTime, windowDef, counting()));
        Vertex slidingWin = dag.newVertex("sliding-window", slidingWindow(windowDef, counting()));
        Vertex filter = dag.newVertex("filter", Processors.map(
                (Frame<String, Long> f) ->
                        f.getValue() > THRESHOLD ?
                                new Alert(System.currentTimeMillis(), ALERT_DESC, new PhoneHome(f.getKey())) :
                                null));
        Vertex sink = dag.newVertex("sink", Processors.writeList(SatimulusEngine.WINDOWING_ALERT_LIST_NAME));

        phSource.localParallelism(1);
        phGenerate.localParallelism(1);
        dag
                .edge(between(phSource, phGenerate).oneToMany())
                .edge(between(phGenerate, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByFrame).distributed()
                        .partitioned(PhoneHome::getOrg, HASH_CODE))
                .edge(between(groupByFrame, slidingWin).partitioned(Frame<Object, Object>::getKey)
                        .distributed())
                .edge(between(slidingWin, filter).oneToMany())
                .edge(between(filter, sink).oneToMany());

        return dag;
    }
}
