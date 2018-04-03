package com.hashmap.tempus.processors;

import java.util.ArrayList;
import java.util.List;

public class GatewayValue {

    private String deviceName;
    private List<DataValue> dataValues;


    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public List<DataValue> getDataValues(){
        return dataValues;
    }

    public GatewayValue(){
        dataValues = new ArrayList<>();
    }

    public void addDataValue(DataValue value){
        dataValues.add(value);
    }
}