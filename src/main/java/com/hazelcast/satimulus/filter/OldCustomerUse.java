package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.satimulus.main.GeneratePhP;
import com.hazelcast.satimulus.main.SatimulusEngine;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.satimulus.main.SatimulusEngine.phPerSecPerMember;

public class OldCustomerUse {
    static final String ALERT_DESC = "Phonehome from an OLD customer!";
    private static final String OLD_CUSTOMERS_FILE_PATH = "C:\\Users\\Alparslan\\workspace\\satimulusengine\\src\\main\\resources\\oldCustomers.txt";

    public static DAG createDag() {
        DAG dag = new DAG();

        Vertex phSource = dag.newVertex("ph-source", Processors.readList(SatimulusEngine.PHONEHOME_LIST_NAME));
        Vertex phGenerate = dag.newVertex("generate-ph", GeneratePhP.generatePh(phPerSecPerMember));
        Vertex oldCustomerSource = dag.newVertex("oldCustomer-source", () -> new FileP(OLD_CUSTOMERS_FILE_PATH));
        Vertex filter = dag.newVertex("filter", OldCustomerFilterP::new);
        Vertex sink = dag.newVertex("sink", Processors.writeList(SatimulusEngine.FILTERING_ALERT_LIST_NAME));

        phSource.localParallelism(1);
        phGenerate.localParallelism(1);
        oldCustomerSource.localParallelism(1);

        dag.edge(between(phSource, phGenerate));
        dag.edge(between(oldCustomerSource, filter).broadcast().priority(-1));
        dag.edge(from(phGenerate).to(filter, 1));
        dag.edge(between(filter, sink));

        return dag;
    }
}
