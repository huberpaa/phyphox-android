package de.rwth_aachen.phyphox.NetworkConnection;

import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import org.apache.poi.util.IOUtils;
import org.eclipse.paho.android.service.MqttAndroidClient;
import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import de.rwth_aachen.phyphox.ExperimentTimeReference;
import de.rwth_aachen.phyphox.R;

import static android.os.Environment.getExternalStorageDirectory;

public class NetworkService {

    enum ResultEnum {
        success, timeout, noConnection, conversionError, genericError
    }

    public static class ServiceResult {
        ResultEnum result;
        String message;
        ServiceResult(ResultEnum result, String message) {
            this.result = result;
            this.message = message;
        }
    }

    public interface RequestCallback {
        void requestFinished(ServiceResult result);
    }

    interface HttpTaskCallback {
        void requestFinished(HttpTaskResult result);
    }

    public static abstract class Service {
        public abstract void connect(String address);
        public abstract void disconnect();
        public abstract void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks);
        public abstract byte[][] getResults();
    }

    static class HttpTaskParameters {
        Map<String, NetworkConnection.NetworkSendableData> send;
        List<RequestCallback> requestCallbacks;
        HttpTaskCallback taskCallback;
        String address;
        boolean usePost;
    }

    static class HttpTaskResult {
        ServiceResult result;
        byte[] data;
        HttpTaskResult(ServiceResult result, byte[] data) {
            this.result = result;
            this.data = data;
        }
        List<RequestCallback> requestCallbacks;
    }

    private static class HttpTask extends AsyncTask<HttpTaskParameters, Void, HttpTaskResult> {
        HttpTaskParameters parameters;
        HttpURLConnection connection;

        public void cancel() {
            cancel(true);
            if (connection != null)
                connection.disconnect();
        }

        @Override
        protected HttpTaskResult doInBackground(HttpTaskParameters... params) {
            DecimalFormat longformat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
            longformat.applyPattern("############0.000");
            longformat.setGroupingUsed(false);

            parameters = params[0];

            try {
                Uri uri = Uri.parse(parameters.address);

                if (!parameters.usePost) {
                    for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : parameters.send.entrySet()) {
                        String value = "";
                        if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                            value = item.getValue().metadata.get(parameters.address);
                        else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER)
                            value = String.valueOf(item.getValue().buffer.value);
                        else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME)
                            value = String.valueOf(longformat.format(System.currentTimeMillis()/1000.0));
                        uri = uri.buildUpon().appendQueryParameter(item.getKey(), value).build();
                    }
                }

                URL url = new URL(uri.toString());

                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod(parameters.usePost ? "POST" : "GET");

                if (parameters.usePost) {
                    connection.setRequestProperty("Content-Type", "application/json; utf-8");
                    connection.setRequestProperty("Accept", "application/json");
                    connection.setDoOutput(true);

                    JSONObject json = new JSONObject();
                    for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : parameters.send.entrySet()) {
                        if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                            json.put(item.getKey(), item.getValue().metadata.get(parameters.address));
                        else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER) {
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
                                        jsonArray.put(v);;
                                }
                                json.put(item.getKey(), jsonArray);
                            }
                        } else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME) {
                            JSONObject timeInfo = new JSONObject();
                            timeInfo.put("now", System.currentTimeMillis()/1000.0);
                            JSONArray events = new JSONArray();
                            for (ExperimentTimeReference.TimeMapping timeMapping : item.getValue().timeReference.timeMappings) {
                                JSONObject eventJson = new JSONObject();
                                eventJson.put("event", timeMapping.event.name());
                                eventJson.put("experimentTime", timeMapping.experimentTime);
                                eventJson.put("systemTime", timeMapping.systemTime/1000.);
                                events.put(eventJson);
                            }
                            timeInfo.put("events", events);
                            json.put(item.getKey(), timeInfo);
                        }
                    }

                    OutputStream os = connection.getOutputStream();
                    byte[] input = json.toString().getBytes("utf-8");
                    os.write(input, 0, input.length);
                    os.close();
                }


                int responseCode = connection.getResponseCode();
                if (200 > responseCode || 300 <= responseCode) {
                    connection.disconnect();
                    return new HttpTaskResult(new ServiceResult(ResultEnum.genericError,"Http response code: " + responseCode), null);
                }

                InputStream is = connection.getInputStream();
                byte[] data = IOUtils.toByteArray(is);
                is.close();
                connection.disconnect();

                return new HttpTaskResult(new ServiceResult(ResultEnum.success,null), data);



            } catch (MalformedURLException e) {
                return new HttpTaskResult(new ServiceResult(ResultEnum.genericError,"No valid URL."), null);
            } catch (IOException e) {
                return new HttpTaskResult(new ServiceResult(ResultEnum.genericError,"IOException."), null);
            } catch (JSONException e) {
                return new HttpTaskResult(new ServiceResult(ResultEnum.genericError,"Could not build JSON."), null);
            }
        }

        @Override
        protected void onPostExecute(HttpTaskResult result) {
            result.requestCallbacks = parameters.requestCallbacks;
            parameters.taskCallback.requestFinished(result);
        }
    }

    public static class HttpGet extends Service implements HttpTaskCallback {
        String address = null;
        byte[] data = null;

        public void connect(String address) {
            this.address = address;
        }

        public void disconnect() {
            address = null;
        }

        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            if (address == null) {
                HttpTaskResult result = new HttpTaskResult(new ServiceResult(ResultEnum.noConnection, null), null);
                for (RequestCallback callback : requestCallbacks) {
                    callback.requestFinished(result.result);
                }
                return;
            }

            HttpTaskParameters parameters = new HttpTaskParameters();
            parameters.requestCallbacks = requestCallbacks;
            parameters.taskCallback = this;
            parameters.address = address;
            parameters.send = send;
            parameters.usePost = false;
            new HttpTask().execute(parameters);
        }

        public byte[][] getResults() {
            byte[][] ret = new byte[1][];
            ret[0] = data;
            return ret;
        }

        public void requestFinished(HttpTaskResult result) {
            data = result.data;
            for (RequestCallback callback : result.requestCallbacks) {
                callback.requestFinished(result.result);
            }
        }
    }

    public static class HttpPost extends Service implements HttpTaskCallback {
        String address = null;
        byte[] data = null;

        public void connect(String address) {
            this.address = address;
        }

        public void disconnect() {
            address = null;
        }

        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            if (address == null) {
                HttpTaskResult result = new HttpTaskResult(new ServiceResult(ResultEnum.noConnection, null), null);
                for (RequestCallback callback : requestCallbacks) {
                    callback.requestFinished(result.result);
                }
                return;
            }

            HttpTaskParameters parameters = new HttpTaskParameters();
            parameters.requestCallbacks = requestCallbacks;
            parameters.taskCallback = this;
            parameters.address = address;
            parameters.send = send;
            parameters.usePost = true;
            new HttpTask().execute(parameters);
        }

        public byte[][] getResults() {
            byte[][] ret = new byte[1][];
            ret[0] = data;
            return ret;
        }

        public void requestFinished(HttpTaskResult result) {
            data = result.data;
            for (RequestCallback callback : result.requestCallbacks) {
                callback.requestFinished(result.result);
            }
        }
    }

    public static abstract class MqttService extends Service {
        List<byte[]> data = new ArrayList<>();
        String receiveTopic;
        String clientID;
        String address;
        MqttAndroidClient client = null;
        Context context;
        boolean connected = false;
        boolean subscribed = false;
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();

        public void connect(String address) {
            this.address = address;
            client = new MqttAndroidClient(context, address, clientID);
            client.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    connected = true;
                    if (!receiveTopic.isEmpty()) {
                        try {
                            client.subscribe(receiveTopic, 0, null, new IMqttActionListener() {
                                @Override
                                public void onSuccess(IMqttToken asyncActionToken) {
                                    subscribed = true;
                                }

                                @Override
                                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                                    subscribed = false;
                                    Log.e("MQTT", "Connection failure: " + exception.getMessage());
                                }
                            });

                        } catch (MqttException ex) {
                            Log.e("MQTT", "Could not subscribe: " + ex.getMessage());
                        }
                    }
                }

                @Override
                public void connectionLost(Throwable cause) {
                    connected = false;
                    subscribed = false;
                    Log.e("MQTT", "Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    data.add(message.getPayload());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
            });

            mqttConnectOptions.setAutomaticReconnect(true);
            mqttConnectOptions.setCleanSession(true);

            try {
                client.connect(mqttConnectOptions, null, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        connected = true;
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        connected = false;
                        subscribed = false;
                        Log.e("MQTT", "Connection failed: " + exception.getMessage());
                    }
                });


            } catch (MqttException ex){
                Log.e("MQTT", "Could not connect: " + ex.getMessage());
            }
        }

        public void disconnect() {
            try {
                client.disconnect();
            } catch (MqttException ex){
                Log.e("MQTT","Could not disconnect: " +  ex.getMessage());
            }
            client = null;
            connected = false;
            subscribed = false;
        }

        public byte[][] getResults() {
            byte[][] ret = data.toArray(new byte[data.size()][]);
            data.clear();
            return ret;
        }
    }

    public static class MqttTlsCsv extends MqttService {
        public MqttTlsCsv(String receiveTopic,
                          String bksFilePath,
                          String userName,
                          String password,
                          Context context) {

            this.receiveTopic = receiveTopic;
            this.context = context;
            this.clientID = "phyphox_" + String.format("%06x", (System.nanoTime() & 0xffffff));

            try {
                KeyStore trustStore = KeyStore.getInstance("BKS");// Read the BKS file we generated (droidstore.bks)
                bksFilePath = getExternalStorageDirectory().getAbsolutePath()+ bksFilePath;

                File bksFile = new File(bksFilePath);
                InputStream input = new FileInputStream(bksFile);
                trustStore.load(input, null);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);

                SSLContext sslCtx = SSLContext.getInstance("TLS");
                sslCtx.init(null, tmf.getTrustManagers(), null);
                mqttConnectOptions.setSocketFactory(sslCtx.getSocketFactory());
            }catch (Exception ex){
                Log.e("MQTT", "TLS CSV: " + ex.toString());
            }

            mqttConnectOptions.setUserName(userName);
            mqttConnectOptions.setPassword(password.toCharArray());
            clientID = userName;
        }

        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            MqttHelper.sendCsv(connected,subscribed,receiveTopic,address,send,requestCallbacks,client);
        }
    }

    public static class MqttCsv extends MqttService {
        public MqttCsv(String receiveTopic, Context context) {
            this.receiveTopic = receiveTopic;
            this.context = context;
            this.clientID = "phyphox_" + String.format("%06x", (System.nanoTime() & 0xffffff));
        }

        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            MqttHelper.sendCsv(connected,subscribed,receiveTopic,address,send,requestCallbacks,client);
        }
    }

    public static class MqttTlsJson extends MqttService{
        String sendTopic;

        public MqttTlsJson(String receiveTopic,
                           String sendTopic,
                           String bksFilePath,
                           String userName,
                           String password,
                           Context context
                           ) {

            this.receiveTopic = receiveTopic;
            this.sendTopic = sendTopic;
            this.context = context;
            this.clientID = "phyphox_" + String.format("%06x", (System.nanoTime() & 0xffffff));

            try {
                KeyStore trustStore = KeyStore.getInstance("BKS");// Read the BKS file we generated (droidstore.bks)
                bksFilePath = getExternalStorageDirectory().getAbsolutePath()+ bksFilePath;

                File bksFile = new File(bksFilePath);
                InputStream input = new FileInputStream(bksFile);
                trustStore.load(input, null);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);

                SSLContext sslCtx = SSLContext.getInstance("TLS");
                sslCtx.init(null, tmf.getTrustManagers(), null);
                mqttConnectOptions.setSocketFactory(sslCtx.getSocketFactory());
            }catch (Exception ex){
                Log.e("MQTT", "TLS Json: " + ex.toString());
            }

            mqttConnectOptions.setUserName(userName);
            mqttConnectOptions.setPassword(password.toCharArray());
            clientID = userName;
        }

        @Override
        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            MqttHelper.sendJson(connected,subscribed,receiveTopic,sendTopic,address,send,requestCallbacks,client);
        }
    }

    public static class MqttJson extends MqttService {
        String sendTopic;

        public MqttJson(String receiveTopic, String sendTopic, Context context) {
            this.receiveTopic = receiveTopic;
            this.sendTopic = sendTopic;
            this.context = context;
            this.clientID = "phyphox_" + String.format("%06x", (System.nanoTime() & 0xffffff));
        }

        public void execute(Map<String, NetworkConnection.NetworkSendableData> send, List<RequestCallback> requestCallbacks) {
            MqttHelper.sendJson(connected,subscribed,receiveTopic,sendTopic,address,send,requestCallbacks,client);
        }
    }

    public static class MqttHelper{

        public static void sendJson (boolean connected,
                                     boolean subscribed,
                                     String receiveTopic,
                                     String sendTopic,
                                     String address,
                                     Map<String, NetworkConnection.NetworkSendableData> send,
                                     List<RequestCallback> requestCallbacks,
                                     MqttAndroidClient client) {

            ServiceResult result;
            if (!connected) {
                result = new ServiceResult(ResultEnum.noConnection, null);
            } else if (!subscribed && !receiveTopic.isEmpty()) {
                result = new ServiceResult(ResultEnum.genericError, "Not subscribed.");
            } else {
                try {
                    JSONObject json = new JSONObject();
                    for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : send.entrySet()) {
                        if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                            json.put(item.getKey(), item.getValue().metadata.get(address));
                        else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER) {
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
                                        jsonArray.put(v);;
                                }
                                json.put(item.getKey(), jsonArray);
                            }
                        } else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME) {
                            JSONObject timeInfo = new JSONObject();
                            timeInfo.put("now", System.currentTimeMillis()/1000.0);
                            JSONArray events = new JSONArray();
                            for (ExperimentTimeReference.TimeMapping timeMapping : item.getValue().timeReference.timeMappings) {
                                JSONObject eventJson = new JSONObject();
                                eventJson.put("event", timeMapping.event.name());
                                eventJson.put("experimentTime", timeMapping.experimentTime);
                                eventJson.put("systemTime", timeMapping.systemTime/1000.);
                                events.put(eventJson);
                            }
                            timeInfo.put("events", events);
                            json.put(item.getKey(), timeInfo);
                        }
                    }

                    MqttMessage message = new MqttMessage();
                    message.setPayload(json.toString().getBytes());
                    client.publish(sendTopic, message);

                    result = new ServiceResult(ResultEnum.success, "");
                } catch (MqttException e) {
                    Log.e("MQTT","Could not publish: " +  e.getMessage());
                    result = new ServiceResult(ResultEnum.genericError, "Could not publish. " + e.getMessage());
                } catch (JSONException e) {
                    Log.e("MQTT","Could not build JSON: " +  e.getMessage());
                    result = new ServiceResult(ResultEnum.genericError, "Could not build JSON. " + e.getMessage());
                }
            }


            for (RequestCallback callback : requestCallbacks) {
                callback.requestFinished(result);
            }
        }
        public static void sendCsv (boolean connected,
                                    boolean subscribed,
                                    String receiveTopic,
                                    String address,
                                    Map<String, NetworkConnection.NetworkSendableData> send,
                                    List<RequestCallback> requestCallbacks,
                                    MqttAndroidClient client) {

            DecimalFormat longformat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
            longformat.applyPattern("############0.000");
            longformat.setGroupingUsed(false);

            ServiceResult result;
            if (!connected) {
                result = new ServiceResult(ResultEnum.noConnection, null);
            } else if (!subscribed && !receiveTopic.isEmpty()) {
                result = new ServiceResult(ResultEnum.genericError, "Not subscribed.");
            } else {
                try {
                    for (Map.Entry<String, NetworkConnection.NetworkSendableData> item : send.entrySet()) {
                        String payload;
                        if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.METADATA)
                            payload = item.getValue().metadata.get(address);
                        else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.BUFFER) {
                            String datatype = item.getValue().additionalAttributes != null ? item.getValue().additionalAttributes.get("datatype") : null;
                            if (datatype != null && datatype.equals("number")) {
                                if (item.getValue().buffer.getFilledSize() == 0)
                                    continue;
                                double v = item.getValue().buffer.value;
                                if (Double.isNaN(v) || Double.isInfinite(v))
                                    payload = "null";
                                else
                                    payload = String.valueOf(v);
                            } else {
                                StringBuilder sb = new StringBuilder();
                                boolean first = true;
                                for (double v : item.getValue().buffer.getArray()) {
                                    if (first)
                                        first = false;
                                    else
                                        sb.append(",");
                                    if (Double.isNaN(v) || Double.isInfinite(v))
                                        sb.append("null");
                                    else
                                        sb.append(v);
                                }
                                payload = sb.toString();
                            }
                        } else if (item.getValue().type == NetworkConnection.NetworkSendableData.DataType.TIME)
                            payload = String.valueOf(longformat.format(System.currentTimeMillis()/1000.0));
                        else
                            continue;
                        MqttMessage message = new MqttMessage();
                        message.setPayload(payload.getBytes());
                        client.publish(item.getKey(), message);
                    }
                    result = new ServiceResult(ResultEnum.success, "");
                } catch (MqttException e) {
                    Log.e("MQTT","Could not publish: " +  e.getMessage());
                    result = new ServiceResult(ResultEnum.genericError, "Could not publish. " + e.getMessage());
                }
            }


            for (RequestCallback callback : requestCallbacks) {
                callback.requestFinished(result);
            }
        }
    }

}
