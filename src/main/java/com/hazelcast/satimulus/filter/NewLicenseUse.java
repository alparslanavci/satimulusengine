package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.satimulus.main.GeneratePhP;
import com.hazelcast.satimulus.main.SatimulusEngine;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.satimulus.main.SatimulusEngine.phPerSecPerMember;

public class NewLicenseUse {
    static final String ALERT_DESC = "Phonehome from new licensed customer!";
    private static final String LICENSES_FILE_PATH = "C:\\Users\\Alparslan\\workspace\\satimulusengine\\src\\main\\resources\\licenses.txt";

    public static DAG createDag() {
        DAG dag = new DAG();

        Vertex phSource = dag.newVertex("ph-source", Processors.readList(SatimulusEngine.PHONEHOME_LIST_NAME));
        Vertex phGenerate = dag.newVertex("generate-ph", GeneratePhP.generatePh(phPerSecPerMember));
        Vertex licenseSource = dag.newVertex("license-source", () -> new FileP(LICENSES_FILE_PATH));
        Vertex filter = dag.newVertex("filter", LicenseFilterP::new);
        Vertex sink = dag.newVertex("sink", Processors.writeList(SatimulusEngine.FILTERING_ALERT_LIST_NAME));

        phSource.localParallelism(1);
        phGenerate.localParallelism(1);
        licenseSource.localParallelism(1);

        dag.edge(between(phSource, phGenerate));
        dag.edge(between(licenseSource, filter).broadcast().priority(-1));
        dag.edge(from(phGenerate).to(filter, 1));
        dag.edge(between(filter, sink));

        return dag;
    }
}
