/*
  Copyright (c) Lightstreamer Srl
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

let stocksGrid= null;
let lsClient= null;
let itemsList = ["flights-[key=10]", "flights-[key=1]", "flights-[key=2]", "flights-[key=3]", "flights-[key=4]", "flights-[key=5]", "flights-[key=6]", "flights-[key=7]", "flights-[key=8]", "flights-[key=9]" ];
let fieldsList = ["key", "destination", "departure", "flightNo", "terminal", "status", "airline", "currentTime"];


function main() {

    // Connect to Lightstreamer Server
    let protocolToUse= document.location.protocol != "file:" ? document.location.protocol : "http:";
    let portToUse= document.location.protocol == "https:" ? LS_HTTPS_PORT : LS_HTTP_PORT;

    lsClient= new Ls.LightstreamerClient(protocolToUse + "//" + LS_HOST + ":" + portToUse, LS_ADAPTER_SET);

    lsClient.addListener(new Ls.StatusWidget("left", "0px", true));

    // Subscribe to Flights Monitor
    
    let dynaGrid = new Ls.DynaGrid("flights", true);

    let watch = new Ls.StaticGrid("currtime", true);

    dynaGrid.setNodeTypes(["div","span","img","a"]);
    dynaGrid.setAutoCleanBehavior(true, false);
    dynaGrid.setSort("departure");
    dynaGrid.addListener({
      onVisualUpdate: function(_key,info) {
          if (info == null) {
            //cleaning
            return;
          }
  
          const cold = "#dedede";
          
          info.setAttribute("lightgreen", cold, "backgroundColor");
      }
      });
    
    let subMonitor = new Ls.Subscription("MERGE",itemsList,fieldsList);
    subMonitor.setDataAdapter("AirpotDemo");
    
    subMonitor.addListener(dynaGrid);
    
    subMonitor.addListener({
      onItemUpdate: function(updateInfo) {
        console.log("New - " + updateInfo.getValue("key") + ", " + updateInfo.getValue("flightNo"));
        
        /*
        dynaGrid.updateRow(updateInfo.getValue("key")-1, {destination:updateInfo.getValue("destination"),
        departure:updateInfo.getValue("departure"),flightno:updateInfo.getValue("flightNo"),
        airline:updateInfo.getValue("airline"),terminal:updateInfo.getValue("terminal"),status:updateInfo.getValue("status")});
        */
        watch.updateRow("time", {currentTime:updateInfo.getValue("currentTime")})
      }
    });
    
    lsClient.subscribe(subMonitor);
    
    lsClient.connect();
    
}

main();