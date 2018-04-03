package com.hashmap.tempus.processors;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class GenerateTimeSeriesFlowfileTest {

    @Test
    public void testGenerateTimeSeries() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateTimeSeriesFlowFile());
        runner.setProperty(GenerateTimeSeriesFlowFile.SIMULATOR_CONFIG, getClass().getResource("/configs/basicConfig.json").getPath());
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateTimeSeriesFlowFile.SUCCESS, 1);
    }

    @Test
    public void testInvalidConfig() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateTimeSeriesFlowFile());
        runner.setProperty(GenerateTimeSeriesFlowFile.SIMULATOR_CONFIG, "/my/invalid/path");
        runner.assertNotValid();
    }

    @Test
    public void testFalseHeaderCreation() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateTimeSeriesFlowFile());
        runner.setProperty(GenerateTimeSeriesFlowFile.PRINT_HEADER, "false");
        runner.setProperty(GenerateTimeSeriesFlowFile.SIMULATOR_CONFIG, getClass().getResource("/configs/unitTestConfig.json").getPath());
        runner.setProperty(GenerateTimeSeriesFlowFile.DATA_FORMAT, "CSV");
        runner.run();
        runner.assertTransferCount(GenerateTimeSeriesFlowFile.SUCCESS, 1);
        runner.getFlowFilesForRelationship(GenerateTimeSeriesFlowFile.SUCCESS).get(0).assertContentEquals("test,2016-01-01T00:00:00.000,17.5");
    }

    @Test
    public void testGatewayMessageType() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateTimeSeriesFlowFile());
        runner.setProperty(GenerateTimeSeriesFlowFile.PRINT_HEADER, "false");
        runner.setProperty(GenerateTimeSeriesFlowFile.LONG_TIMESTAMP, "true");
        runner.setProperty(GenerateTimeSeriesFlowFile.SIMULATOR_CONFIG, getClass().getResource("/configs/unitTestConfig.json").getPath());
        runner.setProperty(GenerateTimeSeriesFlowFile.DATA_FORMAT, "JSON");
        runner.setProperty(GenerateTimeSeriesFlowFile.DEVICE_NAME, "test");
        runner.setProperty(GenerateTimeSeriesFlowFile.JSON_DEVICE_TYPE, "Gateway");
        runner.run();
        runner.assertTransferCount(GenerateTimeSeriesFlowFile.SUCCESS, 1);
        runner.getFlowFilesForRelationship(GenerateTimeSeriesFlowFile.SUCCESS).get(0).assertContentEquals("{\"test\":[{\"values\":{\"test\":\"17.5\"},\"ts\":\"1451628000000\"}]}");
    }
}
