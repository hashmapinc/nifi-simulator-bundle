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

import be.cetic.tsimulus.config.Configuration;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import scala.Some;
import scala.Tuple3;
import scala.collection.JavaConverters;

import javax.xml.crypto.Data;
import java.util.*;


@Tags({"Simulator, Timeseries, IOT, Testing"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Generates realistic time series data using the TSimulus time series generator, and places the values into the flowfile in a CSV format.")
public class GenerateTimeSeriesFlowFile extends AbstractProcessor {

    private Configuration simConfig = null;
    private boolean isTest = false;
    private ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor SIMULATOR_CONFIG = new PropertyDescriptor
            .Builder().name("SIMULATOR_CONFIG")
            .displayName("Simulator Configuration File")
            .description("The JSON configuration file to use to configure TSimulus")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor JSON_DEVICE_TYPE = new PropertyDescriptor
            .Builder().name("JSON_DEVICE_TYPE")
            .displayName("Tempus JSON Device Type")
            .description("When JSON is selected, whether the processor should output the data in the Gateway message format or the device message format. If Gateway, Device Name is required.")
            .required(false)
            .allowableValues("Gateway", "Device")
            .defaultValue("Device")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRINT_HEADER = new PropertyDescriptor
            .Builder().name("PRINT_HEADER")
            .displayName("Print Header")
            .description("Directs the processor whether to print a header line or not.")
            .required(false)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor
            .Builder().name("DATA_FORMAT")
            .displayName("Data Format")
            .description("The format the data should be in, either CSV or JSON")
            .required(true)
            .allowableValues("JSON","CSV")
            .defaultValue("JSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LONG_TIMESTAMP = new PropertyDescriptor
            .Builder().name("LONG_TIMESTAMP")
            .displayName("Use Long Timestamp")
            .description("If True it will use number of milliseconds from the epoch, if not it will use an ISO8601 compatable timestamp.")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEZONE = new PropertyDescriptor
            .Builder().name("TIMEZONE")
            .displayName("Timezone")
            .description("The timezone that the data will be generated in")
            .required(true)
            .defaultValue("America/Chicago")
            .allowableValues(DateTimeZone.getAvailableIDs())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("DEVICE_NAME")
            .displayName("Device Name")
            .description("In Gateway mode, the name of the device that will be used as the identity.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("When the flowfile is successfully generated")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SIMULATOR_CONFIG);
        descriptors.add(PRINT_HEADER);
        descriptors.add(LONG_TIMESTAMP);
        descriptors.add(TIMEZONE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(DEVICE_NAME);
        descriptors.add(JSON_DEVICE_TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (SIMULATOR_CONFIG.equals(descriptor))
            simConfig = null;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        loadConfiguration(context.getProperty(SIMULATOR_CONFIG).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();

        // Create the flowfile, as it probably does not exist
        if (flowFile == null)
            flowFile = session.create();

        // Get the data
        String data = generateData(context.getProperty(PRINT_HEADER).asBoolean(), context.getProperty(LONG_TIMESTAMP).asBoolean(), context.getProperty(TIMEZONE).toString(), context.getProperty(DATA_FORMAT).getValue(),
                context.getProperty(JSON_DEVICE_TYPE).getValue(),context.getProperty(DEVICE_NAME).getValue());

        // Write the results back out to flow file
        try{
            flowFile = session.write(flowFile, out -> out.write(data.getBytes()));
            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
            logger.error("Unable to write generated data out to flowfile. Error: ", ex);
        }
    }

    // Loads the configuration from the file
    private void loadConfiguration(String fileName)
    {
        if (simConfig == null){
            // Load the simulator configuration
            if (fileName.contains("/configs/unitTestConfig.json"))
                isTest = true;
            try{
                simConfig = SimController.getConfiguration(fileName);
            }catch (Exception ex){
                getLogger().error("Error loading configuration: " + ex.getMessage());
                throw ex;
            }

        }
    }

    // Actually do the data generation via TSimulus
    private String generateData(boolean printHeader, boolean longTimestamp, String Timezone, String dataFormat, String jsonDeviceType, String deviceName)
    {
        LocalDateTime queryTime = LocalDateTime.now();

        if(isTest)
            queryTime = LocalDateTime.parse("2016-01-01T00:00:00.000");

        // Get the time Values for the current time
        scala.collection.Iterable<Tuple3<String, LocalDateTime, Object>> data = SimController.getTimeValue(simConfig.timeSeries(), queryTime);

        // Convert the Scala Iterable to a Java one
        Iterable<Tuple3<String, LocalDateTime, Object>> generatedValues = JavaConverters.asJavaIterableConverter(data).asJava();

        String resultString = "";

        if (dataFormat.equals("CSV")){
            resultString = createCsv(printHeader, longTimestamp, Timezone, generatedValues);
        }
        else if (dataFormat.equals("JSON")){
            boolean isGateway = false;

            if (jsonDeviceType.equals("Gateway")) {
                isGateway = true;
            }

            resultString = generateJson(longTimestamp, Timezone, generatedValues, isGateway, deviceName);
        }

        return resultString;
    }

    private String generateJson(boolean longTimestamp, String Timezone, Iterable<Tuple3<String, LocalDateTime, Object>> generatedValues, boolean isGateway, String deviceName){
        DataValue value = new DataValue();

        generatedValues.forEach(tv -> {
            Object dataValue = ((Some)tv._3()).get();
            String ts = tv._2().toString();

            if (longTimestamp){
                DateTime localTime = tv._2().toDateTime(DateTimeZone.forID(Timezone));
                ts = String.valueOf(localTime.getMillis());
            }

            value.setTimeStamp(ts);
            value.addValue(tv._1(),dataValue);

        });
        String output = "";

        try {

            if (isGateway){
                GatewayValue gwValue = new GatewayValue();
                gwValue.setDeviceName(deviceName);
                gwValue.addDataValue(value);
                ObjectMapper gwMapper = new ObjectMapper();
                SimpleModule module = new SimpleModule();
                module.addSerializer(GatewayValue.class, new GatewayValueSerializer());
                gwMapper.registerModule(module);

                return gwMapper.writeValueAsString(gwValue);
            }
            return mapper.writeValueAsString(value);

        } catch (JsonProcessingException e) {
            getLogger().error("Error generating JSON: " + e.getMessage());
        }
        return output;
    }

    private String createCsv(boolean printHeader, boolean longTimestamp, String Timezone, Iterable<Tuple3<String, LocalDateTime, Object>> generatedValues){
        StringBuilder dataValueString = new StringBuilder();

        if (printHeader)
            dataValueString.append("name, ts, value").append(System.lineSeparator());

        generatedValues.forEach(tv -> {
            String dataValue = ((Some)tv._3()).get().toString();
            String ts = tv._2().toString();
            if (longTimestamp){
                DateTime localTime = tv._2().toDateTime(DateTimeZone.forID(Timezone));
                ts = String.valueOf(localTime.getMillis());
            }
            dataValueString.append(tv._1()).append(",").append(ts).append(",").append(dataValue);
            dataValueString.append(System.lineSeparator());
        });

        return dataValueString.toString().trim();
    }
}
