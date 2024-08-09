{
  "name": "${name}",
  "version": "${version}",
  "title": "${title}",
  "description": "The Kafka Connect Lightstreamer Sink connector is used to move data from Kafka into Lightstreamer.",
  "owner": {
    "username": "${owner}",
    "type": "organization",
    "name": "Lightstreamer Srl",
    "url": "https://lightstreamer.com",
    "logo": "assets/lightstreamer.jpg"
  },
  "support": {
    "summary": "Lightstreamer provides full support for this connector.",
    "logo": "assets/lightstreamer.jpg",
    "url": "https://lightstreamer.com/contact",
    "provider_name": "Lightstreamer Srl"
  },
  "tags": [ "Lightstreamer", "realtime", "${name}", "push", "websockets", "bandwidth", "routing", "filtering", "delivery" ],
  "features": {
    "supported_encodings": [ "any" ],
    "single_message_transforms": true,
    "confluent_control_center_integration": true,
    "kafka_connect_api": true
  },
  "logo": "assets/lightstreamer.jpg",
  "documentation_url": "https://github.com/Lightstreamer/Lightstreamer-kafka-connector",
  "source_url" : "https://github.com/Lightstreamer/Lightstreamer-kafka-connector",  
  "docker_image" : {
    "name" : "lightstreamer"
  },
  "license": [
    {
      "name": "Apache License, Version 2.0",
      "url": "http://www.apache.org/licenses/LICENSE-2.0"
    }
  ],  
  "component_types": [ "sink" ],
  "release_date" : "${release_date}"
}
