/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.client.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.ActivityClusterId;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobId;
import org.json.JSONException;

public class ActivityClusterGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(ActivityClusterGraphBuilder.class.getName());

    public ActivityClusterGraphBuilder() {
    }

    private static Pair<ActivityId, ActivityId> findMergePair(JobActivityGraph jag, Set<Set<ActivityId>> eqSets) {
        for (Set<ActivityId> eqSet : eqSets) {
            for (ActivityId t : eqSet) {
                List<IConnectorDescriptor> inputList = jag.getActivityInputMap().get(t);
                if (inputList != null) {
                    for (IConnectorDescriptor conn : inputList) {
                        ActivityId inTask = jag.getProducerActivity(conn.getConnectorId());
                        if (!eqSet.contains(inTask)) {
                            //                            System.out.println("output <<--- input found.");
                            return Pair.<ActivityId, ActivityId> of(t, inTask);
                        }
                    }
                }
                List<IConnectorDescriptor> outputList = jag.getActivityOutputMap().get(t);
                if (outputList != null) {
                    for (IConnectorDescriptor conn : outputList) {
                        ActivityId outTask = jag.getConsumerActivity(conn.getConnectorId());
                        if (!eqSet.contains(outTask)) {
                            //                            System.out.println("input --> output found.");
                            return Pair.<ActivityId, ActivityId> of(t, outTask);
                        }
                    }
                }
            }
        }
        return null;
    }

    public ActivityClusterGraph inferActivityClusters(JobId jobId, JobActivityGraph jag) {
        /*
         * Build initial equivalence sets map. We create a map such that for each IOperatorTask, t -> { t }
         */
        Map<ActivityId, Set<ActivityId>> stageMap = new HashMap<ActivityId, Set<ActivityId>>();
        Set<Set<ActivityId>> stages = new HashSet<Set<ActivityId>>();
        for (ActivityId taskId : jag.getActivityMap().keySet()) {
            Set<ActivityId> eqSet = new HashSet<ActivityId>();
            eqSet.add(taskId);
            stageMap.put(taskId, eqSet);
            stages.add(eqSet);
        }

        boolean changed = true;
        while (changed) {
            changed = false;
            Pair<ActivityId, ActivityId> pair = findMergePair(jag, stages);
            if (pair != null) {
                //                System.out.println("inferActivityClusters - stages: " + stages + "\npair: " + pair.getLeft() + " "
                //                        + jag.getActivityMap().get(pair.getLeft()) + " <<---->> " + pair.getRight() + " "
                //                        + jag.getActivityMap().get(pair.getRight()));
            }
            if (pair != null) {
                merge(stageMap, stages, pair.getLeft(), pair.getRight());
                //                System.out
                //                        .println("inferActivityClusters - stageMap: " + stageMap + "\n" + "stages:" + stages + "\n\n");
                changed = true;
            }
        }

        ActivityClusterGraph acg = new ActivityClusterGraph();
        Map<ActivityId, ActivityCluster> acMap = new HashMap<ActivityId, ActivityCluster>();
        int acCounter = 0;
        Map<ActivityId, IActivity> activityNodeMap = jag.getActivityMap();
        List<ActivityCluster> acList = new ArrayList<ActivityCluster>();
        for (Set<ActivityId> stage : stages) {
            ActivityCluster ac = new ActivityCluster(acg, new ActivityClusterId(jobId, acCounter++));
            //            System.out.println("new AC:" + ac);
            acList.add(ac);
            for (ActivityId aid : stage) {
                IActivity activity = activityNodeMap.get(aid);
                ac.addActivity(activity);
                //                System.out.println("activity:" + activity + " added.");
                acMap.put(aid, ac);
            }
            //            System.out.println("\n");
        }

        //        System.out.println("The number of AC: " + acList.size() + "\n");

        for (Set<ActivityId> stage : stages) {
            for (ActivityId aid : stage) {
                IActivity activity = activityNodeMap.get(aid);
                ActivityCluster ac = acMap.get(aid);
                List<IConnectorDescriptor> aOutputs = jag.getActivityOutputMap().get(aid);
                if (aOutputs == null || aOutputs.isEmpty()) {
                    ac.addRoot(activity);
                    //                    System.out.println("ac:" + ac + "\naddRoot: " + activity + "\n");
                } else {
                    int nActivityOutputs = aOutputs.size();
                    for (int i = 0; i < nActivityOutputs; ++i) {
                        IConnectorDescriptor conn = aOutputs.get(i);
                        ac.addConnector(conn);
                        //                        System.out.println("connector added: " + conn);
                        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> pcPair = jag.getConnectorActivityMap()
                                .get(conn.getConnectorId());
                        ac.connect(conn, activity, i, pcPair.getRight().getLeft(), pcPair.getRight().getRight(), jag
                                .getConnectorRecordDescriptorMap().get(conn.getConnectorId()));
                        //                        System.out.println("connected: pro: " + activity + " port: " + i + " cons: "
                        //                                + pcPair.getRight().getLeft() + " port: " + pcPair.getRight().getRight() + "\n");
                    }
                }
            }
        }

        Map<ActivityId, Set<ActivityId>> blocked2BlockerMap = jag.getBlocked2BlockerMap();
        for (ActivityCluster s : acList) {
            Map<ActivityId, Set<ActivityId>> acBlocked2BlockerMap = s.getBlocked2BlockerMap();
            Set<ActivityCluster> blockerStages = new HashSet<ActivityCluster>();
            for (ActivityId t : s.getActivityMap().keySet()) {
                Set<ActivityId> blockerTasks = blocked2BlockerMap.get(t);
                acBlocked2BlockerMap.put(t, blockerTasks);
                if (blockerTasks != null) {
                    for (ActivityId bt : blockerTasks) {
                        blockerStages.add(acMap.get(bt));
                        //                        System.out.println("ac: " + s + "\nblockerStages: " + blockerStages + " block Activity:" + bt
                        //                                + "\n");
                    }
                }
            }
            for (ActivityCluster bs : blockerStages) {
                s.getDependencies().add(bs);
                if (s.equals(bs)) {
                    throw new IllegalStateException(
                            "ActivityClusterGraphBuilder.inferActivityClusters: an activity cluster cannot be registered as its dependency. The plan needs to be revised.");
                }
                //                System.out.println("ac: " + s + " dependency added: " + bs);
            }
        }
        acg.addActivityClusters(acList);

        if (LOGGER.isLoggable(Level.FINE)) {
            try {
                LOGGER.fine(acg.toJSON().toString(2));
            } catch (JSONException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return acg;
    }

    private void merge(Map<ActivityId, Set<ActivityId>> eqSetMap, Set<Set<ActivityId>> eqSets, ActivityId t1,
            ActivityId t2) {
        Set<ActivityId> stage1 = eqSetMap.get(t1);
        Set<ActivityId> stage2 = eqSetMap.get(t2);

        Set<ActivityId> mergedSet = new HashSet<ActivityId>();
        mergedSet.addAll(stage1);
        mergedSet.addAll(stage2);

        eqSets.remove(stage1);
        eqSets.remove(stage2);
        eqSets.add(mergedSet);

        for (ActivityId t : mergedSet) {
            eqSetMap.put(t, mergedSet);
        }
    }
}