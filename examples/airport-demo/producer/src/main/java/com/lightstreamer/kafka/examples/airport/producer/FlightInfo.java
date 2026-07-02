
/*
 * Copyright (C) 2026 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.examples.airport.producer;

import com.fasterxml.jackson.annotation.JsonValue;

public class FlightInfo {

    enum FlightStatus {
        SCHEDULED_ON_TIME("Scheduled - On-time", "\uD83C\uDFAB"),
        SCHEDULED_DELAYED("Scheduled - Delayed", "⌛"),
        EN_ROUTE_ON_TIME("En Route - On-time", "\uD83D\uDEEB"),
        EN_ROUTE_DELAYED("En Route - Delayed", "\uD83D\uDEEC"),
        LANDED_ON_TIME("Landed - On-time", "✅"),
        LANDED_DELAYED("Landed - Delayed", "\uD83D\uDFE9"),
        CANCELLED("Cancelled", "\uD83D\uDED1"),
        DELETED("Deleted", "\uD83D\uDED1");

        private final String description;
        private final String icon;

        FlightStatus(String description, String icon) {
            this.description = description;
            this.icon = icon;
        }

        public String getIcon() {
            return icon;
        }

        @JsonValue
        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private final String flightNo;

    private final String destination;

    private final String terminal;

    private final String airline;

    private final String scheduled;

    private String effective;

    private FlightStatus status;

    public FlightInfo(
            String flightNo,
            String destination,
            String terminal,
            String airline,
            String scheduled,
            String effective,
            FlightStatus status) {
        this.flightNo = flightNo;
        this.destination = destination;
        this.terminal = terminal;
        this.airline = airline;
        this.scheduled = scheduled;
        this.effective = effective;
        this.status = status;
    }

    public String getFlightNo() {
        return flightNo;
    }

    public String getDestination() {
        return destination;
    }

    public String getTerminal() {
        return terminal;
    }

    public String getAirline() {
        return airline;
    }

    public String getScheduled() {
        return scheduled;
    }

    public String getEffective() {
        return effective;
    }

    public void setEffective(String effective) {
        this.effective = effective;
    }

    public FlightStatus getStatus() {
        return status;
    }

    public void setStatus(FlightStatus status) {
        this.status = status;
    }

    public String getDestinationStr() {
        return status.getIcon() + ' ' + destination;
    }
}
