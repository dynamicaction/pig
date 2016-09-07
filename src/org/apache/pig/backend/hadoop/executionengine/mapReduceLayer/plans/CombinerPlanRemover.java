package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class CombinerPlanRemover extends MROpPlanVisitor {


    public CombinerPlanRemover(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }
    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {


        if(!mr.combinePlan.isEmpty()) {
            mr.combinePlan = new PhysicalPlan();
        }
    }
}
