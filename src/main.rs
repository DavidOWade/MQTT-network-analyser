use std::{
    process,
    thread,
    sync::mpsc,
    time::{Duration}
};

extern crate paho_mqtt as mqtt;

// Connection constants
const BROKER:&str = "tcp://localhost:1883";

// Client details
const USERNAME:&str = "username";
const PASSWORD:&str = "password";

const CTRL_CLIENT:&str = "control";
const CTRL_TOPICS:&[&str] = &["request/qos", "request/delay"];
const CTRL_QOS:&[i32] = &[0, 1, 2];

const PUB_CLIENT:&str = "publisher";
const PUB_TOPIC:&str = "counter";

// Try reconnect after connection loss 
fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("{}: connection lost. Waiting to try again...", CTRL_CLIENT);
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("{}: successfully reconnected", CTRL_CLIENT);
            return true;
        }
    }
    println!("{}: unable to reconnect after several attempts", CTRL_CLIENT);
    false
}

// Subscribe to request topics (control client)
fn subscribe_topics(cli: &mqtt::Client) {
    if let Err(e) = cli.subscribe_many(CTRL_TOPICS, &CTRL_QOS) {
        println!("{}: error subscribing to topics: {}", CTRL_CLIENT, e);
        process::exit(1);
    }
}


fn main() {
    let (pubsub_tx, pubsub_rx) = mpsc::channel();
    
    // Spawn publisher thread
    thread::spawn(move || {
        let mut pub_topic = [PUB_TOPIC, "0", "0"].join("/");
        let mut pub_qos = 0;
        let mut pub_delay = 0;

        // Configure publisher client options
        let pub_create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(BROKER)
            .client_id(PUB_CLIENT.to_string())
            .finalize();
        
        // Create publisher client
        let pub_cli = mqtt::Client::new(pub_create_opts).unwrap_or_else(|err| {
            println!("{}: error creating the client: {}", PUB_CLIENT, err);
            process::exit(1);
        });

        // Connect publisher client
        let pub_conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .user_name(USERNAME)
            .password(PASSWORD)
            .finalize();
        
        // Connect publisher client
        if let Err(e) = pub_cli.connect(pub_conn_opts) {
            println!("{} Unable to connect:\n\t{}", PUB_CLIENT, e);
            process::exit(1);
        } else {
            println!("{}: connected to broker {}", PUB_CLIENT, BROKER);
        }

        let mut recv_state = "err";

        let mut counter:i32 = 0;

        println!("{}: publishing messages on {}", PUB_CLIENT, pub_topic);

        // Publish messages and await IPC from analyser to change state
        while recv_state != "kill" {
            let content = counter.to_string();

            counter += 1;
    
            let msg = mqtt::Message::new(&pub_topic, content.clone(), pub_qos);
    
            // Publish counter
            if let Err(e) = pub_cli.publish(msg) {
                println!("{}: error sending message: {} \nError: {}", PUB_CLIENT, content, e);
                break;
            }
    
            thread::sleep(Duration::from_millis(pub_delay));

            // Check for state change
            recv_state = match pubsub_rx.try_recv() {
                Ok(res) => res,
                Err(_) => "err",
            };


            // Update state
            if (recv_state != "err") & (recv_state != "kill") {
                let opts: Vec<&str> = recv_state.split(":").collect();

                if opts.len() == 2 {
                    if opts[0] == "qos" {
                        pub_qos = opts[1].parse().unwrap();
                        let new_topic = [PUB_TOPIC, pub_qos.to_string().as_str(), pub_delay.to_string().as_str()].join("/");
                        println!("{}: publishing messages on {}", PUB_CLIENT, new_topic);
                        pub_topic = new_topic;
                    } else if opts[0] == "delay" {
                        pub_delay = opts[1].parse().unwrap();
                        let new_topic = [PUB_TOPIC, pub_qos.to_string().as_str(), pub_delay.to_string().as_str()].join("/");
                        println!("{}: publishing messages on {}", PUB_CLIENT, new_topic);
                        pub_topic = [PUB_TOPIC, pub_qos.to_string().as_str(), pub_delay.to_string().as_str()].join("/");
                    }
                }

                recv_state = "err";

            }

        }

        // Disconnect publisher
        if pub_cli.is_connected() {
            println!("{}: disconnecting from broker", PUB_CLIENT);
            pub_cli.disconnect(None).unwrap();
        }
    });
    
    // Configure controller client options
    let ctrl_create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(BROKER)
        .client_id(CTRL_CLIENT.to_string())
        .finalize();
    
    // Create controller client
    let ctrl_cli = mqtt::Client::new(ctrl_create_opts).unwrap_or_else(|err| {
        println!("{}: error creating the client: {}", CTRL_CLIENT, err);
        process::exit(1);
    });

    // Begin listening
    let rx = ctrl_cli.start_consuming();

    let lwt = mqtt::MessageBuilder::new()
        .topic("lostconn")
        .payload("Consumer lost connection")
        .finalize();
    
    // Configure controller connection options
    let ctrl_conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(false)
        .will_message(lwt)
        .user_name(USERNAME)
        .password(PASSWORD)
        .finalize();
    
    // Connect controller client
    if let Err(e) = ctrl_cli.connect(ctrl_conn_opts) {
        println!("{}: unable to connect:\n\t{}", CTRL_CLIENT, e);
        process::exit(1);
    } else {
        println!("{}: connected to broker on {}", CTRL_CLIENT, BROKER);
    }

    // Subscribe to control client options (requests from analyser)
    subscribe_topics(&ctrl_cli);

    println!("{}: listening for requests...", CTRL_CLIENT);

    // Upon message from analyser, update publisher thread
    for msg in rx.iter() {

        if let Some(msg) = msg {

            let val = msg.payload_str();

            let topic = msg.topic();

            println!("{}: received message {:?} on topic {:?}", CTRL_CLIENT, val, topic);

            let mut update_topic = "";

            match topic {
                "request/qos" => (
                    if val == "0" {
                        update_topic = "qos:0";
                    } else if val == "1" {
                        update_topic = "qos:1";
                    } else if val == "2" {
                        update_topic = "qos:2";
                    }
                ),
                "request/delay" => (
                    if val == "0" {
                        update_topic = "delay:0";
                    } else if val == "1" {
                        update_topic = "delay:1";
                    } else if val == "2" {
                        update_topic = "delay:2";
                    } else if val == "10" {
                        update_topic = "delay:10";
                    } else if val == "20" {
                        update_topic = "delay:20";
                    } else if val == "100" {
                        update_topic = "delay:100";
                    } else if val == "200" {
                        update_topic = "delay:200";
                    }
                ),
                _ => panic!("Unexpected topic")
            }

            pubsub_tx.send(update_topic).unwrap();

            


        } else if !ctrl_cli.is_connected() {
            if try_reconnect(&ctrl_cli) {
                println!("{}: resubscribe topics...", CTRL_CLIENT);
                subscribe_topics(&ctrl_cli);
            } else {
                break;
            }
        }
    }

    // Send kill signal to publisher thread
    pubsub_tx.send("kill").unwrap();

    // Disconnect controller
    if ctrl_cli.is_connected() {
        println!("{} disconnecting from broker", CTRL_CLIENT);
        ctrl_cli.unsubscribe_many(CTRL_TOPICS).unwrap();
        ctrl_cli.disconnect(None).unwrap();
    }

    
}
