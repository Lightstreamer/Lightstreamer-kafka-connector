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
let itemsList = [ "flights-board" ];
let fieldsList = ["key", "command", "destination", "departure", "flightNo", "terminal", "status", "airline", "currentTime"];


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
    
    let subMonitor = new Ls.Subscription("COMMAND",itemsList,fieldsList);
    subMonitor.setDataAdapter("AirpotDemo");
	subMonitor.set
    
    subMonitor.addListener(dynaGrid);
    
    subMonitor.addListener({
      onItemUpdate: function(updateInfo) {
        console.log("New - " + updateInfo.getValue("key") + ", " + updateInfo.getValue("flightNo"));
        
        /*
        dynaGrid.updateRow(updateInfo.getValue("key")-1, {destination:updateInfo.getValue("destination"),
        departure:updateInfo.getValue("departure"),flightno:updateInfo.getValue("flightNo"),
        airline:updateInfo.getValue("airline"),terminal:updateInfo.getValue("terminal"),status:updateInfo.getValue("status")});
        */

        const value = updateInfo.getValue("currentTime");
        if (value) {
          watch.updateRow("time", {currentTime:updateInfo.getValue("currentTime")});
        } 
        
      },
	  onClearSnapshot: function(itemName, itemPos) {
		console.log("Clear snapshot for " + itemName);
	  },
	  onEndOfSnapshot: function(itemName, itemPos) {
		console.log("End of snapshot for " + itemName);
	  }
    });
    
    lsClient.subscribe(subMonitor);
    
    lsClient.connect();
    
}

main();