package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.satimulus.main.GeneratePhP;
import com.hazelcast.satimulus.main.SatimulusEngine;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.satimulus.main.SatimulusEngine.phPerSecPerMember;

public class HighPardotScoreUse {
    static final String ALERT_DESC = "Phonehome from a high pardot lead!";
    private static final String TOP100_PARDOT_COMPANIES_FILE_PATH
            = "C:\\Users\\Alparslan\\workspace\\satimulusengine\\src\\main\\resources\\top100pardotCompanies.txt";

    public static DAG createDag() {
        DAG dag = new DAG();

        Vertex phSource = dag.newVertex("ph-source", Processors.readList(SatimulusEngine.PHONEHOME_LIST_NAME));
        Vertex phGenerate = dag.newVertex("generate-ph", GeneratePhP.generatePh(phPerSecPerMember));
        Vertex highPardotSource = dag.newVertex("highPardot-source", () -> new FileP(TOP100_PARDOT_COMPANIES_FILE_PATH));
        Vertex filter = dag.newVertex("filter", HighPardotFilterP::new);
        Vertex sink = dag.newVertex("sink", Processors.writeList(SatimulusEngine.FILTERING_ALERT_LIST_NAME));

        phSource.localParallelism(1);
        phGenerate.localParallelism(1);
        highPardotSource.localParallelism(1);

        dag.edge(between(phSource, phGenerate));
        dag.edge(between(highPardotSource, filter).broadcast().priority(-1));
        dag.edge(from(phGenerate).to(filter, 1));
        dag.edge(between(filter, sink));

        return dag;
    }
}
