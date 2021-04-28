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
package org.apache.hyracks.control.nc.work;

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NotifyTaskCompleteWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();
    private final NodeControllerService ncs;
    private final Task task;

    public NotifyTaskCompleteWork(NodeControllerService ncs, Task task) {
        this.ncs = ncs;
        this.task = task;
    }

    @Override
    public void run() {
        ActivityId activityId = task.getTaskAttemptId().getTaskId().getActivityId();
        String blocker = "";
        ActivityCluster activityCluster = task.getJoblet().getActivityClusterGraph().getActivityMap().get(activityId);
        try {
            if (activityCluster.getBlocked2BlockerMap().size() > 0) {
                if (activityCluster.getBlocked2BlockerMap().containsKey(activityId)) {
                    for (ActivityId anid : activityCluster.getBlocked2BlockerMap().get(activityId)) {
                        blocker += anid;
                    }
                }
            }
        } catch (Exception e) {
            blocker = "error when getting blocker";
        }

        String connectorOutputODID = "";
        if (task.getPartitionSendProfile().entrySet().size() > 0) {
            connectorOutputODID += activityCluster.getActivityInputMap().keySet().toArray()[0].toString();
        }

        TaskProfile taskProfile = new TaskProfile(task.getTaskAttemptId(), blocker, connectorOutputODID, task.getPartitionSendProfile(),
                task.getStatsCollector(), task.getWarnings(), task.getWarningCollector().getTotalWarningsCount());
        try {
            ncs.getClusterController(task.getJobletContext().getJobId().getCcId()).notifyTaskComplete(
                    task.getJobletContext().getJobId(), task.getTaskAttemptId(), ncs.getId(), taskProfile);
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Failed notifying task complete for " + task.getTaskAttemptId(), e);
        }
        task.getJoblet().removeTask(task);
    }

    @Override
    public String toString() {
        return getName() + ": [" + ncs.getId() + "[" + task.getJoblet().getJobId() + ":" + task.getTaskAttemptId()
                + "]";
    }
}
