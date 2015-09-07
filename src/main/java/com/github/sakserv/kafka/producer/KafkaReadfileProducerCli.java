/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.kafka.producer;

import com.github.sakserv.kafka.producer.config.ConfigVars;
import com.github.sakserv.propertyparser.PropertyParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaReadfileProducerCli {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReadfileProducerCli.class);

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("USAGE: java jar </path/to/producer.jar> </path/to/propsfile> </path/to/inputfile>");
            System.exit(1);
        }

        // Setup the property parser
        PropertyParser propertyParser = new PropertyParser(args[0]);
        propertyParser.parsePropsFile();

        // Producer
        KafkaReadfileProducer kafkaReadfileProducer = new KafkaReadfileProducer.Builder()
                .setKafkaHostname(propertyParser.getProperty(ConfigVars.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)))
                .setTopic(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY))
                .setInputFileName(args[1])
                .build();

        kafkaReadfileProducer.produceMessages();

    }

}
