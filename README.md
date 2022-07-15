# mqtt-network-analyser
Publisher/controller/analyser model for testing MQTT networks (with variable delay and QOS)
---

The program includes two important binaries: the publisher/controller and the analyser.

To compile the binaries, use

`cargo build`

Navigate to `target/debug` and run the programs `mqtt` and `analyser`

`./mqtt` will run the publisher and controller

`./analyser` will run the analyser 

Dependencies: [paho-mqtt](https://github.com/eclipse/paho.mqtt.rust)