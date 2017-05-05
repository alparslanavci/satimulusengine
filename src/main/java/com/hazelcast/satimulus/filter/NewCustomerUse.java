package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.satimulus.main.GeneratePhP;
import com.hazelcast.satimulus.main.SatimulusEngine;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.satimulus.main.SatimulusEngine.phPerSecPerMember;

public class NewCustomerUse {
    static final String ALERT_DESC = "Phonehome from a new customer!";
    private static final String EXISTING_CUSTOMERS_FILE_PATH = "C:\\Users\\Alparslan\\workspace\\satimulusengine\\src\\main\\resources\\existingCustomers.txt";

    public static DAG createDag() {
        DAG dag = new DAG();

        Vertex phSource = dag.newVertex("ph-source", Processors.readList(SatimulusEngine.PHONEHOME_LIST_NAME));
        Vertex phGenerate = dag.newVertex("generate-ph", GeneratePhP.generatePh(phPerSecPerMember));
        Vertex existingCustomerSource = dag.newVertex("existingCustomer-source", () -> new FileP(EXISTING_CUSTOMERS_FILE_PATH));
        Vertex filter = dag.newVertex("filter", NewCustomerFilterP::new);
        Vertex sink = dag.newVertex("sink", Processors.writeList(SatimulusEngine.FILTERING_ALERT_LIST_NAME));

        phSource.localParallelism(1);
        phGenerate.localParallelism(1);
        existingCustomerSource.localParallelism(1);

        dag.edge(between(phSource, phGenerate));
        dag.edge(between(existingCustomerSource, filter).broadcast().priority(-1));
        dag.edge(from(phGenerate).to(filter, 1));
        dag.edge(between(filter, sink));

        return dag;
    }
}
