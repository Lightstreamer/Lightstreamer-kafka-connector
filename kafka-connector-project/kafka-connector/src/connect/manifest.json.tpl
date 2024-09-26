{
  "name": "${name}",
  "version": "${version}",
  "title": "${title}",
  "description": "The Lightstreamer Kafka Connector is an intelligent proxy that bridges Kafka with web and mobile applications, ensuring real-time data streaming across the last mile. It addresses challenges like firewalls, unpredictable internet bandwidth, and large data volumes using Lightstreamer’s advanced technology.\n\nBy eliminating the need for REST polling and MQTT proxies, it simplifies architecture while enhancing performance with features like Intelligent Streaming, adaptive throttling, server-side filtering, and powerful topic mapping.\n\nDesigned for massive scalability, Lightstreamer supports millions of concurrent clients with low latency. It efficiently fans out real-time messages from Kafka topics, minimizing the load on both the Kafka broker and the client applications.\n\nThe Lightstreamer Kafka Connector can be configured to work either as a Kafka Connect sink or a direct Kafka client.\n\nIt provides extensive client SDKs for developing applications for the web, mobile, desktop, and various smart devices.\n\n\n\nTags: last-mile, websockets, ws, http, apps, frontend, front end, front-end, javascript, html, android, ios, swift, broadcast, push, push notifications",
  "owner": {
    "username": "${owner}",
    "type": "organization",
    "name": "Lightstreamer Srl",
    "url": "https://lightstreamer.com/",
    "logo": "assets/lightstreamer.png"
  },
  "support": {
    "summary": "Lightstreamer provides full support for this component.",
    "logo": "assets/lightstreamer.png",
    "url": "https://lightstreamer.com/contact",
    "provider_name": "Lightstreamer Srl"
  },
  "tags": [ "Lightstreamer", "kafka-connect-lightstreamer", "last mile", "last-mile", "lastmile", "intelligent", "intelligent streaming", "websocket", "websockets", "ws", "web", "mobile", "apps", "front end", "front-end", "frontend", "javascript", "html", "android", "ios", "swift", "offloading", "fan out", "fanout", "scaling", "scalability", "scalable", "broadcast", "broadcasting", "rest", "mqtt", "firewall", "proxy", "filter", "filtering", "push", "push notifications", "real time", "real-time", "realtime", "bandwidth", "routing", "web streamng", "internet", "remote", "wan", "push technology", "http", "http streaming", "polling", "long polling", "optimize", "optimization"],
  "features": {
    "supported_encodings": [ "any" ],
    "single_message_transforms": true,
    "confluent_control_center_integration": true,
    "kafka_connect_api": true
  },
  "logo": "assets/lightstreamer.png",
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
