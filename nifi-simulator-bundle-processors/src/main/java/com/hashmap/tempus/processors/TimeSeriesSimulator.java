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
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.joda.time.LocalDateTime;
import scala.Some;
import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.JavaConverters;

import java.util.*;

@Tags({"Simulator, Timeseries, IOT, Testing"})
@CapabilityDescription("Generates realistic time series data with the TSimulus time series generator.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class TimeSeriesSimulator extends AbstractProcessor {

    public static final PropertyDescriptor SIMULATOR_CONFIG = new PropertyDescriptor
            .Builder().name("SIMULATOR_CONFIG")
            .displayName("Simulator Configuration File")
            .description("The JSON configuration file to use to configure TSimulus")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("When the flowfile is successfully generated")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Success")
            .description("When the flowfile is successfully generated")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SIMULATOR_CONFIG);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();

        String configText = context.getProperty(SIMULATOR_CONFIG).getValue();

        Configuration config = SimUtils.getConfiguration(configText);

        // Get the time Values for the current time
        Iterable<Tuple3<String, LocalDateTime, Object>> data = SimUtils.getTimeValue(config.timeSeries());

        // Convert the Scala Iterable to a Java one
        java.lang.Iterable<Tuple3<String, LocalDateTime, Object>> values = JavaConverters.asJavaIterableConverter(data).asJava();

        StringBuilder dataValueString = new StringBuilder();

        // Build the flow file string
        values.forEach(tv -> {
            String dataValue = ((Some)tv._3()).get().toString();
            dataValueString.append(tv._1()).append(",").append(tv._2().toString()).append(",").append(dataValue);
            dataValueString.append(System.lineSeparator());
        });

        if (flowFile == null)
            flowFile = session.create();

        // Write the results back out to flow file
        try{
            flowFile = session.write(flowFile, out -> out.write(dataValueString.toString().trim().getBytes()));
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
            logger.error("Unable to process", ex);
            session.transfer(flowFile, FAILURE);
        }
    }
}
