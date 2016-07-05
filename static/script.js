$(function() {
    var loc = window.location;
    var uri = 'ws://localhost:8080/log/stream';

    function streamInitFn(token, streamId) {
        id = sendLog(token, {
            "name":"33th is name",
            "body":"thdy"
        });
        queryId = sendQuery(token, streamId, {
            "nameLike":"%",
            "bodyRegex":"^this.*$",
            "newerThan": 0
        });
        for (var i = 0; i < 100; i++) {
            id = sendLog(token, {
                "name":"one",
                "body":"two"
            });
            id = sendLog(token, {
                "name":"this is name",
                "body":"this is body"
            });
        }
        unsubscribe(token, queryId);
        id = sendLog(token, {
            "name":"this is name",
            "body":"this is body. and should not appear."
        });
        id = sendLog(token, {
            "name":"this is name. should not appear.",
            "body":"this is body. and should not appear."
        });
        queryId = sendQuery(token, streamId, {
            "nameLike":"%",
            "bodyRegex":"^this.*$",
            "newerThan": 0
        });
        id = sendLog(token, {
            "name":"this is name.",
            "body":"this is body. this should show up."
        });
    }

    function initWebsocket(token) {
        Cookies.set('accessToken', token, { expires: 1 });
        ws = new WebSocket(uri);

        ws.onopen = function() {
            console.log('Connected')
        }

        ws.onclose = function() {
            console.log('Closed')
        }

        ws.onmessage = function(evt) {
            //console.log(evt.data);
            var out = document.getElementById('output');
            out.innerHTML += evt.data + '<br>';
            var obj = JSON.parse(evt.data);
            if (obj['ping']) {
                ws.send(JSON.stringify({
                    pong: obj['ping']
                }));
                console.log('Sent pong')
            } else if (obj['streamId']) {
                streamInitFn(token, obj['streamId']);
            }
        }
    }

    function login(accessKey) {
        var data = JSON.stringify({ accessKey: accessKey });
        var token = '';
        $.ajax({
            type: 'POST',
            url: 'http://localhost:8080/login',
            data: data,
            success: function(data) {
                token = data['accessToken'];
                console.log('token: ' + token)
            },
            contentType: "application/json",
            dataType: 'json',
            async: false
        });
        return token;
    }

    function sendLog(token, message) {
        var elapsed = 0;
        var messageId = 0;
        $.ajax({
            type: 'POST',
            url: 'http://localhost:8080/log/new',
            data: JSON.stringify(message),
            beforeSend: function(xhr){xhr.setRequestHeader('Authorization', 'Bearer '+ token);},
            success: function(data) {
                elapsed = data['elapsed'];
                messageId = data['messageId'];
                console.log('elapsed: ' + elapsed)
            },
            contentType: "application/json",
            dataType: 'json',
            async: false
        });
        return messageId;
    }

    function sendQuery(token, streamId, message) {
        var elapsed = 0;
        var queryId = 0;
        // TODO(daeyun): copy
        message['targetStreamId'] = streamId;

        $.ajax({
            type: 'POST',
            url: 'http://localhost:8080/log/subscribe',
            data: JSON.stringify(message),
            beforeSend: function(xhr){xhr.setRequestHeader('Authorization', 'Bearer '+ token);},
            success: function(data) {
                elapsed = data['elapsed'];
                queryId = data['queryId'];
                console.log('elapsed: ' + elapsed)
            },
            contentType: "application/json",
            dataType: 'json',
            async: false
        });
        return queryId;
    }

    function unsubscribe(token, queryId) {
        var elapsed = 0;
        // TODO(daeyun): copy
        var message = {}
        message['queryId'] = queryId;

        $.ajax({
            type: 'POST',
            url: 'http://localhost:8080/log/unsubscribe',
            data: JSON.stringify(message),
            beforeSend: function(xhr){xhr.setRequestHeader('Authorization', 'Bearer '+ token);},
            success: function(data) {
                elapsed = data['elapsed'];
                console.log('elapsed: ' + elapsed)
            },
            contentType: "application/json",
            dataType: 'json',
            async: false
        });
        return elapsed;
    }


    token = login("123");
    initWebsocket(token);

    //var query = {
        // accessToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE0Njc1ODM3MzJ9.MSooniWpKGz1U9PrhkW8u5K2poysTQ0kHo6WHTTOf_s',
        //accessToken: 'eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE0Njc2NzcyMDN9.9P-Qa3FwVMYpt3vSy_3neSQp7ZpShXFWvXwKv2vudeubFHot_lkiDKmCJ2G86pL1',
        //regex: 'hi2',
        //startTime: 42,
    //}

    //setInterval(function() {
    //ws.send(JSON.stringify(query));
    //}, 1000);
});
