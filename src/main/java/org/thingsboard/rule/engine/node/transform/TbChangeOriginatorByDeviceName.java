/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;
import static org.thingsboard.rule.engine.api.TbRelationTypes.FAILURE;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "Change originator by Device name",
        configClazz = TbChangeOriginatorByDeviceNameConfiguration.class,
        nodeDescription = "Change orginator by Device name of the telemetry data. ",
        nodeDetails = "",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbEnrichmentNodeSumIntoMetadataConfig")
public class TbChangeOriginatorByDeviceName implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private TbChangeOriginatorByDeviceNameConfiguration config;
    private String inputKey;
    private String prefix;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbChangeOriginatorByDeviceNameConfiguration.class);
        inputKey = config.getInputKey();
        prefix = config.getPrefix();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        boolean hasRecords = false;
        EntityId id = null;
        Device device = null;
        try {
            JsonNode jsonNode = mapper.readTree(msg.getData());
            Iterator<String> iterator = jsonNode.fieldNames();
            while (iterator.hasNext()) {
                String field = iterator.next();
                if (field.startsWith(inputKey)) {
                    String name = prefix + jsonNode.get(field);
                    device = ctx.getDeviceService().findDeviceByTenantIdAndName(ctx.getTenantId(), name.replace("\"", ""));
                    if (device != null) {
                        hasRecords = true;
                        id = device.getId();
                        System.out.println("####" + name.replace("\"", "") + " | " + id + "####");
                    }
                    break;
                }
            }
            if (hasRecords) {
                TbMsg newMsg = TbMsg.transformMsg(msg, msg.getType(), id, msg.getMetaData(), msg.getData());
                ctx.tellNext(newMsg, SUCCESS);
            } else {
                TbMsgMetaData metadata = msg.getMetaData();
                String name = prefix + metadata.getValue(inputKey);
                if (name != null) {
                    device = ctx.getDeviceService().findDeviceByTenantIdAndName(ctx.getTenantId(), name.replace("\"", ""));
                    if (device != null) {
                        id = device.getId();
                        hasRecords = true;
                        System.out.println("####" + name.replace("\"", "") + " | " + id + "####");
                    }
                }
            }
            if (hasRecords) {
                TbMsg newMsg = TbMsg.transformMsg(msg, msg.getType(), id, msg.getMetaData(), msg.getData());
                ctx.tellNext(newMsg, SUCCESS);
            } else {
                //ctx.tellFailure(msg, new Exception("Message doesn't contain the key: " + inputKey));
                ctx.tellNext(msg, FAILURE);
            }
        } catch (IOException e) {
            ctx.tellFailure(msg, e);
        }
    }

    @Override
    public void destroy() {
    }
}