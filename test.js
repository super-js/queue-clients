var mqtt = require('mqtt')
var client  = mqtt.connect('mqtt://192.168.1.11:1883', {
    username: 'user',
    password: 'password'
})

client.on('connect', function () {
    client.subscribe('test/topicNo1', function (err) {

        if(err) console.error(err);

        // if(err) {
        //     console.log(err)
        // } else {
        //     client.publish('job', 'Hello mqtt')
        // }
    })
})

client.on('message', function (topic, message) {
    // message is Buffer
    console.log(topic, message.toString())
    //client.end()
})

// const fins = require('omron-fins');
// const client = fins.FinsClient(9600,'192.168.250.3', {SA1:1, DA1:1});
//
// // Setting up our error listener
// client.on('error',function(error, seq) {
//     console.log("Error: ", error, seq);
// });
// // Setting up our timeout listener
// client.on('timeout',function(host, seq) {
//     console.log("Timeout: ", host);
// });
//
// // Setting up the genral response listener
// // Showing a selection of properties of a sequence response
// client.on('reply',function(seq) {
//     console.log("Reply from           : ", seq.response.remoteHost);
//     console.log("Sequence ID (SID)    : ", seq.sid);
//     console.log("Operation requested  : ", seq.request.functionName);
//     console.log("Response code        : ", seq.response.endCode);
//     console.log("Response desc        : ", seq.response.endCodeDescription);
//     console.log("Data returned        : ", seq.response.values || "");
//     console.log("Round trip time      : ", seq.timeTaken + "ms");
//     console.log("Your tag: ", seq.tag);
// });
//
// // Read 10 registers starting at DM register 0
// // a "reply" will be emitted - check general client reply on reply handler
// client.read('D700',10);
//
// // Read 10 registers starting at DM register D700 & callback with my tagged item upon reply from PLC
// // direct callback is usefull for getting direct responses to direct requests
// // var cb = function(err, seq) {
// //     console.log("############# DIRECT CALLBACK #################")
// //     if(err)
// //         console.error(err);
// //     else
// //         console.log(seq.request.functionName, seq.response.values);
// //     console.log("###############################################")
// // };
// // client.read('D700',10, cb, new Date());
// //
// // //example fill D700~D704 with 123
// // client.fill('D700',123, 5);
// //
// // //example tagged data for sending with a status request
// // var tag = {"source": "system-a", "sendto": "system-b"};
// //
// // client.status(function(err, seq) {
// //     if(err) {
// //         console.error(err);
// //     } else {
// //         //use the tag for post reply routing
// //         console.log(seq.tag, seq.response);
// //     }
// // }, tag);