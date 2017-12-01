$(document).ready(function () {


var ws = new WebSocket("ws://localhost:8002/ws");

ws.addEventListener("message", function(e){
    var msg = JSON.parse(e.data)
    $("#messages").append(msg + "\n");
})

$("#send-message").click(function(){

    var data = $("#message").val()
    console.log("Clicked " + data);
    ws.send(JSON.stringify({msg: data}));
});
});