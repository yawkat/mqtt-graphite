#!/usr/bin/python3

import datetime
import json
import traceback

import graphitesend
import paho.mqtt.client


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--graphite-server")
    parser.add_argument("--mqtt-server")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    args = parser.parse_args()

    print("Initializing graphite...")
    graphitesend.init(graphite_server=args.graphite_server, prefix="mqtt", system_name="")

    last_by_device = {}

    def on_msg(msg: paho.mqtt.client.MQTTMessage):
        nonlocal last_by_device

        topic = msg.topic
        if not topic.startswith("gosund/"):
            return
        parts = topic.split('/', 3)
        if len(parts) < 4:
            return
        (devp1, topic, devp2, t) = parts
        if topic != "tele":
            return
        prefix = devp1 + "." + devp2
        if t == "SENSOR":
            payload = json.loads(msg.payload)
            energy = payload["ENERGY"]

            data = {
                prefix + ".power": energy["Power"],
                prefix + ".current": energy["Current"],
                prefix + ".power_factor": energy["Factor"],
                prefix + ".voltage": energy["Voltage"],
            }

            # kWh -> Ws
            total = energy["Total"] * 1000 * 3600
            time = datetime.datetime.strptime(payload["Time"], "%Y-%m-%dT%H:%M:%S")
            if devp2 in last_by_device:
                dev_data = last_by_device[devp2]
            else:
                dev_data = {}
                last_by_device[devp2] = dev_data

            if "time" in dev_data and dev_data["time"] < time and dev_data["total"] <= total:
                data[prefix + ".power2"] = \
                    (total - dev_data["total"]) / (time - dev_data["time"]).total_seconds()
            else:
                print("Fixing time to " + str(time) + " from " + str(dev_data))
            dev_data["time"] = time
            dev_data["total"] = total

            graphitesend.send_dict(data)
        elif t == "STATE":
            payload = json.loads(msg.payload)
            graphitesend.send_dict({
                prefix + ".vcc": payload["Vcc"],
            })

    def on_msg0(cl, userdata, msg: paho.mqtt.client.MQTTMessage):
        try:
            on_msg(msg)
        except:
            traceback.print_exc()

    client = paho.mqtt.client.Client()
    client.on_message = on_msg0
    print("Connecting mqtt...")
    client.connect(args.mqtt_server, args.mqtt_port)
    print("Subscribing...")
    client.subscribe("gosund/#", 0)
    print("Subscribed!")
    while True:
        client.loop()


if __name__ == '__main__':
    main()
