package de.rwth_aachen.phyphox.NetworkConnection.Mqtt;

import android.util.Log;

import com.devsmart.ubjson.UBObject;
import com.devsmart.ubjson.UBValueFactory;
import com.devsmart.ubjson.UBWriter;

import org.apache.poi.util.ArrayUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.stream.Stream;

import de.rwth_aachen.phyphox.ExperimentTimeReference;
import de.rwth_aachen.phyphox.NetworkConnection.NetworkConnection;

public final class JsonHelper {

    public static UBObject buildJson(Map<String, NetworkConnection.NetworkSendableData> send, //todo UB
                                       MqttService mqttService){

            UBObject ubJsonObject = UBValueFactory.createObject();

            for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : send.entrySet()) {
                if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                    ubJsonObject.put(item.getKey(), UBValueFactory.createValue(item.getValue().metadata.get(mqttService.address)));
                else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER) {
                    if(mqttService.clearBuffer){
                        mqttService.experiment.dataLock.lock();
                        try{
                            ubJsonObject = writeBufferValuesIntoUBJson(item,ubJsonObject);
                            item.getValue().buffer.clear(false);
                        } finally {
                            mqttService.experiment.dataLock.unlock();
                        }
                    }else {
                        ubJsonObject = writeBufferValuesIntoUBJson(item,ubJsonObject);
                    }
                } else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME) {
                    UBObject timeInfo = UBValueFactory.createObject();
                    timeInfo.put("now", UBValueFactory.createValue(System.currentTimeMillis() / 1000.0));
                    JSONArray events = new JSONArray();

                    for (ExperimentTimeReference.TimeMapping timeMapping : item.getValue().timeReference.timeMappings) {
                        UBObject eventJson = UBValueFactory.createObject();
                        eventJson.put("event", UBValueFactory.createValue(timeMapping.event.name()));
                        eventJson.put("experimentTime", UBValueFactory.createValue(timeMapping.experimentTime));
                        eventJson.put("systemTime", UBValueFactory.createValue(timeMapping.systemTime / 1000.));
                        events.put(eventJson);
                    }
                    timeInfo.put("events", UBValueFactory.createArray(events.toString().getBytes()));
                    ubJsonObject.put(item.getKey(), timeInfo);
                }
            }
            return ubJsonObject;
    }

    private static UBObject writeBufferValuesIntoUBJson(Map.Entry<String, NetworkConnection.NetworkSendableData> item,
                                                    UBObject ubJsonObject)
    {
        String datatype = item.getValue().additionalAttributes != null ? item.getValue().additionalAttributes.get("datatype") : null;
        if (datatype != null && datatype.equals("number")) {
            double v = item.getValue().buffer.value;
            if (Double.isNaN(v) || Double.isInfinite(v))
                ubJsonObject.put(item.getKey(), null);
            else
                ubJsonObject.put(item.getKey(), UBValueFactory.createValue(v));
        } else {
            Double[] bufferArray = item.getValue().buffer.getArray();
            double[] doubleArray = new double[bufferArray.length];
            for (int i = 0; i < bufferArray.length; i++) {
                if (Double.isNaN(bufferArray[i]) || Double.isInfinite(bufferArray[i]))
                    doubleArray[i] = 0;
                else
                    doubleArray[i] = bufferArray[i];
            }
            ubJsonObject.put(item.getKey(), UBValueFactory.createArray(doubleArray));
        }
        return ubJsonObject;
    }

    public static byte[] prepareUBJsonPayload(UBObject ubJsonObject){
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            UBWriter writer = new UBWriter(out);
            writer.write(ubJsonObject);
            writer.close();
            Log.e("UBJson",out.toString());
            return out.toString().getBytes();
        }catch (Exception ex){
            Log.e("UBJson",ex.toString());
            return ex.toString().getBytes();
        }
    }

    private static void writeBufferValuesIntoJson(Map.Entry<String, NetworkConnection.NetworkSendableData> item,
                                                  JSONObject json)throws JSONException
    {
        String datatype = item.getValue().additionalAttributes != null ? item.getValue().additionalAttributes.get("datatype") : null;
        if (datatype != null && datatype.equals("number")) {
            double v = item.getValue().buffer.value;
            if (Double.isNaN(v) || Double.isInfinite(v))
                json.put(item.getKey(), null);
            else
                json.put(item.getKey(), v);
        } else {
            JSONArray jsonArray = new JSONArray();
            for (double v : item.getValue().buffer.getArray()) {
                if (Double.isNaN(v) || Double.isInfinite(v))
                    jsonArray.put(null);
                else
                    jsonArray.put(v);
            }
            json.put(item.getKey(), jsonArray);
        }
    }

/*
    public static JSONObject buildJson(Map<String, NetworkConnection.NetworkSendableData> send,
                                        MqttService mqttService) throws JSONException {

        JSONObject json = new JSONObject();

        for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : send.entrySet()) {
            if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                json.put(item.getKey(), item.getValue().metadata.get(mqttService.address));
            else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER) {
                if(mqttService.clearBuffer){
                    mqttService.experiment.dataLock.lock();
                    try{
                        writeBufferValuesIntoJson(item,json);
                        item.getValue().buffer.clear(false);
                    } finally {
                        mqttService.experiment.dataLock.unlock();
                    }
                }else {
                    writeBufferValuesIntoJson(item,json);
                }
            } else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME) {
                JSONObject timeInfo = new JSONObject();
                timeInfo.put("now", System.currentTimeMillis() / 1000.0);
                JSONArray events = new JSONArray();
                for (ExperimentTimeReference.TimeMapping timeMapping : item.getValue().timeReference.timeMappings) {
                    JSONObject eventJson = new JSONObject();
                    eventJson.put("event", timeMapping.event.name());
                    eventJson.put("experimentTime", timeMapping.experimentTime);
                    eventJson.put("systemTime", timeMapping.systemTime / 1000.);
                    events.put(eventJson);
                }
                timeInfo.put("events", events);
                json.put(item.getKey(), timeInfo);
            }
        }
        return json;
    }
*/

}
