use std::{
    process,
    thread,
    time::{Duration, Instant}
};

extern crate paho_mqtt as mqtt;

const HOST:&str = "tcp://localhost";
const PORT:&str = "1883";
const USERNAME:&str = "username";
const PASSWORD:&str = "password";
const CLIENT:&str = "analyser";

fn main() {
    let broker = [HOST, PORT].join(":");

    // Configure analyser client options
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(broker)
        .client_id(CLIENT)
        .finalize();
    
    // Create analyser client
    let cli = mqtt::Client::new(create_opts).unwrap_or_else(|err| {
        println!("{:?}: error creating the client: {:?}", CLIENT, err);
        process::exit(1);
    });

    // Start receiving messages (from publisher)
    let rx = cli.start_consuming();

    let lwt = mqtt::MessageBuilder::new()
        .topic("lostconn")
        .payload("Consumer lost connection")
        .finalize();

    // Configure analyser client options
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_millis(20))
        .clean_session(false)
        .will_message(lwt)
        .user_name(USERNAME)
        .password(PASSWORD)
        .finalize();
    
    // Connect analyser client
    if let Err(e) = cli.connect(conn_opts) {
        println!("{}: unable to connect:\n\t{:?}", CLIENT, e);
        process::exit(1);
    } else {
        println!("{}: connected to broker on {:?}", CLIENT, [HOST, PORT].join(":"));
    }

    let mut topics_string: Vec<String> = Vec::new();
    let mut qos_opts: Vec<i32> = Vec::new();
    let mut delay_opts: Vec<&str> = Vec::new();

    // Build list of topics to listen on
    let mut qos = 0;
    for q in ["0", "1", "2"] {
        for d in ["0", "1", "2", "10", "20", "100", "200"] {
            let topic = ["counter", q, d].join("/");
            topics_string.push(topic);
            qos_opts.push(qos);
            delay_opts.push(d);
        }
        qos += 1;
    }

    let topics: Vec<&str> = topics_string.iter().map(AsRef::as_ref).collect();

    let mut topic_idx = 0;

    let mut prev_qos:i32 = 0;
    let mut prev_delay = "0";

    if let Err(e) = cli.subscribe(topics[topic_idx], qos_opts[topic_idx]) {
        println!("{}: error subscribing to topic {}\n\t{}", CLIENT, topics[topic_idx], e);
        process::exit(1);
    } else {
        println!("{}: subscribed to topic {}", CLIENT, topics[topic_idx]);
    }

    if let Err(e) = cli.subscribe("$SYS/#", 1) {
        println!("{}: error subscribing to topic {}\n\t{}", CLIENT, "$SYS/#", e);
        process::exit(1);
    } else {
        println!("{}: subscribed to topic {}", CLIENT, "$SYS/#");
    }

    let start = Instant::now();

    let mut messages: Vec<u64> = Vec::new();
    let mut delays: Vec<u128> = Vec::new();

    let mut prev_time = 0;

    delays.push(0);

    let mut sys_counter = 0;

    println!("{}: collecting requests...", CLIENT);
    println!("{}: awaiting $SYS response", CLIENT);

    // Collect & analyse messages
    for msg in rx.iter() {
        if let Some(msg) = msg {

            let topic = msg.topic().to_string();

            // Handle $SYS messages (only print the relevant ones)
            if &topic[..4] == "$SYS" {
                if sys_counter < 6 {
                    sys_counter += 1;

                    let (_, sub_topic) = topic.split_at(5);
                    let val = msg.payload_str().to_string();

                    match sub_topic {
                        "broker/load/bytes/sent/1min" => println!("{}: {}", topic, val),
                        "broker/load/bytes/received/1min" => println!("{}: {}", topic, val),
                        "broker/load/publish/received/1min" => println!("{}: {}", topic, val),
                        "broker/load/publish/sent/1min" => println!("{}: {}", topic, val),
                        "broker/load/publish/dropped/1min" => println!("{}: {}", topic, val),
                        "broker/clients/active" => println!("{}: {}", topic, val),
                        "broker/heap/current" => println!("{}: {}", topic, val),
                        _ => sys_counter -= 1
                    }
                }

                prev_time = start.elapsed().as_millis();
                continue;
                
            }

            let val: u64 = msg.payload_str().to_string().parse().unwrap();

            messages.push(val);

            let now = start.elapsed().as_millis();

            delays.push(now - prev_time);

            prev_time = now;


            if (now % 10000 < 1000) & (start.elapsed().as_millis() > 1000) {
                let (mr, lr, ooor, dmean, dmed) =  analyse(&messages, &delays);
                println!();
                println!("Message rate: {} per second", mr);
                println!("Loss rate: {}", lr);
                println!("Out-of-order rate: {}", ooor);
                println!("Inter-message mean: {} milliseconds", dmean);
                println!("Inter-message median: {} milliseconds", dmed);
                println!();
                thread::sleep(Duration::from_millis(1000));

                if let Err(e) = cli.unsubscribe(topics[topic_idx]) {
                    println!("{}: failed to unsubscribed from topic {}\r\t{}", CLIENT, topics[topic_idx], e);
                } else {
                    println!("{}: unsubscribed from topic {}", CLIENT, topics[topic_idx]);
                }

                topic_idx = (topic_idx + 1) % topics.len();

                if let Err(e) = cli.subscribe(topics[topic_idx], qos_opts[topic_idx]) {
                    println!("{}: error subscribing to topic {}\r\t{}", CLIENT, topics[topic_idx], e);
                    process::exit(1);
                } else {
                    println!("{}: subscribed to topic {}\n", CLIENT, topics[topic_idx]);
                }

                if qos_opts[topic_idx] != prev_qos {
                    let msg = mqtt::Message::new("request/qos", qos_opts[topic_idx].to_string(), 1);
                    cli.publish(msg).unwrap();
                    prev_qos = qos_opts[topic_idx];
                }

                if delay_opts[topic_idx] != prev_delay {
                    let msg = mqtt::Message::new("request/delay", delay_opts[topic_idx].to_string(), 1);
                    cli.publish(msg).unwrap();
                    prev_delay = delay_opts[topic_idx];
                }

                println!("{}: awaiting $SYS response", CLIENT);

                sys_counter = 0;

                messages.clear();
                delays.clear();
                delays.push(0);
            }


        } else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("{}: resubscribe topics...", CLIENT);
                if let Err(e) = cli.subscribe(topics[topic_idx], qos_opts[topic_idx]) {
                    println!("{}: error subscribing to topic {}\r\t{}", CLIENT, topics[topic_idx], e);
                    process::exit(1);
                } else {
                    println!("{}: subscribed to topic {}", CLIENT, topics[topic_idx]);
                }
            } else {
                break;
            }
        }

        
    }

    if let Err(e) = cli.unsubscribe("$SYS/#") {
        println!("{}: failed to unsubscribed from topic {}\r\t{}", CLIENT, "$SYS/#", e);
    } else {
        println!("{}: unsubscribed from topic {}", CLIENT, "$SYS/#");
    }

    if cli.is_connected() {
        println!("{}: disconnecting...", CLIENT);
        cli.unsubscribe(topics[topic_idx]).unwrap();
        cli.disconnect(None).unwrap();
    }

    fn analyse(message_vec: &Vec<u64>, message_delays: &Vec<u128>) -> (f32, f32, f32, f32, f32) {
        let n_messages = message_vec.len();

        let max = message_vec.iter().max().unwrap();
        let min = message_vec.iter().min().unwrap();
        let n_messages_expected = max - min;

        println!("actual: {} expected: {}", n_messages, n_messages_expected);
        
        let message_rate:f32 = n_messages as f32 / 10 as f32;
        let loss_rate:f32 = 1.0 - (n_messages as f32 / n_messages_expected as f32);

        let mut delayed_messages = 0;

        for i in 0..n_messages {
            if i != n_messages - 1 {
                let next_msg = message_vec[i+1];
                if next_msg < message_vec[i] {
                    delayed_messages += 1;
                }
            }
        }

        let out_of_order_rate:f32 = delayed_messages as f32 / n_messages as f32;

        let delay_mean: f32 = message_delays.iter().sum::<u128>() as f32 / n_messages as f32;

        let delay_median:f32;

        if n_messages / 2 % 2 == 0 {
            let mid_idx = n_messages / 2;
            delay_median = (message_delays[mid_idx] + message_delays[mid_idx+1]) as f32 / 2 as f32;
        } else {
            let mid_idx = (n_messages as f64 / 2.0).ceil() as usize;
            delay_median = message_delays[mid_idx] as f32;
        }

        (message_rate, loss_rate, out_of_order_rate, delay_mean, delay_median)

    }




}

fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("{}: connection lost. Waiting to try again...", CLIENT);
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(2500));
        if cli.reconnect().is_ok() {
            println!("{}: successfully reconnected", CLIENT);
            return true;
        }
    }
    println!("{}: unable to reconnect after several attempts", CLIENT);
    false
}

