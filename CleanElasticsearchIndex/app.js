var AWS = require('aws-sdk');
var client = new AWS.HttpClient();

exports.handler = function (event, context) {
    cleanIndexes();
};

////////////////////////////////////////////////////////////////
// Parameters Part
////////////////////////////////////////////////////////////////

var parameters = {
    'storageUsageMinThreshold': 75,
    'endpoint': 'testcluster.testregion.es.amazonaws.com',
    'getAllIndexesPath': '_cluster/health',
    'getAllIndexesQuery': 'level=indices',
    'getStoragePath': '_cluster/stats',
    'getStorageQuery': 'human&pretty',
    'indexPrefix': 'cwl-',
    'preserveDays': 7
};

////////////////////////////////////////////////////////////////
// Elasticsearch Operations Part
////////////////////////////////////////////////////////////////

function getStorageUsage() {
    console.log('INFO: Getting storage size');
    console.log('INFO: URL=https://' + parameters.endpoint + '/' + parameters.getStoragePath + '?' + parameters.getStorageQuery);
    var requestParams = buildRequest(
        parameters.endpoint,
        parameters.getStoragePath,
        'GET',
        parameters.getStorageQuery,
        null
    );

    return new Promise(resolve => {
        client.handleRequest(requestParams, null, function (response) {
            var responseBody = '';
            response.on('data', function (chunk) {
                responseBody += chunk;
            });
            response.on('end', function () {
                var info = JSON.parse(responseBody);

                if (response.statusCode == 200) {
                    var data = info['nodes']['fs'];
                    var freeStorage = parseFloat(data['free_in_bytes']);
                    var totalStorage = parseFloat(data['total_in_bytes']);
                    var currentStorage = 100.0 - ((freeStorage / totalStorage) * 100.0);
                    resolve(currentStorage);
                }

                var error = response.statusCode != 200 || info.Message != null ? {
                    "statusCode": response.statusCode,
                    "responseBody": responseBody
                } : null;

                if (error != null) {
                    console.log(JSON.stringify(error, null, 2));
                    resolve(-1);
                }
            });
        }, function (error) {
            console.log(JSON.stringify(error, null, 2));
            resolve(-1);
        });
    });
}

function getIndexes() {
    console.log('INFO: Getting list of indexes');
    console.log('INFO: URL=https://' + parameters.endpoint + '/' + parameters.getAllIndexesPath + '?' + parameters.getAllIndexesQuery);
    var requestParams = buildRequest(
        parameters.endpoint,
        parameters.getAllIndexesPath,
        'GET',
        parameters.getAllIndexesQuery,
        null
    );

    return new Promise(resolve => {
        client.handleRequest(requestParams, null, function (response) {
            var responseBody = '';
            response.on('data', function (chunk) {
                responseBody += chunk;
            });
            response.on('end', function () {
                var info = JSON.parse(responseBody);

                if (response.statusCode == 200) {
                    var indexes = [];

                    Object.keys(info.indices).forEach(function (idx) {
                        if (idx.startsWith(parameters.indexPrefix)) {
                            indexes.push(idx);
                        }
                    });

                    console.log('INFO: Found ' + indexes.length + ' indexes');
                    console.log('INFO: Sorting indexes');
                    indexes.sort();

                    resolve(indexes);
                }

                var error = response.statusCode != 200 || info.Message != null ? {
                    "statusCode": response.statusCode,
                    "responseBody": responseBody
                } : null;

                if (error != null) {
                    console.log(JSON.stringify(error, null, 2));
                    resolve([]);
                }
            });
        }, function (error) {
            console.log(JSON.stringify(error, null, 2));
            resolve([]);
        });
    });
}

async function cleanIndexes() {
    var currentStorage = await getStorageUsage();
    console.log('INFO: Current storage is ' + currentStorage + '%');

    if (currentStorage == -1) {
        console.log('WARN: It was not able to retrieve current storage size');
    }
    else if (currentStorage >= parameters.storageUsageMinThreshold) {
        console.log('INFO: Current storage is above the threshold');

        var indexes = await getIndexes();

        var regexp = new RegExp('^' + parameters.indexPrefix + '(\\d\\d\\d\\d)\\.?(\\d\\d)\\.?(\\d\\d)?$');
        //console.log(regexp);
        var now = new Date();
        var endsTime = getDiffDateTimestamp(
            now.getUTCFullYear(), now.getUTCMonth() + 1, now.getUTCDate(),
            0, 0, parameters.preserveDays);

        console.log('INFO: Script will clean indexes before ' + parameters.preserveDays + ' day(s)');

        var count = 0, total = 0;

        for (var i in indexes) {
            var idx = indexes[i];
            if (idx.startsWith(parameters.indexPrefix)) {
                var matchResult = idx.match(regexp);
                //console.log(JSON.stringify(matchResult, null, 2));
                var year = matchResult[1];
                var month = matchResult[2];
                var day = matchResult[3];
                var indexTime = getDiffDateTimestamp(year, month, day, 0, 0, 0);

                if (indexTime < endsTime) {
                    total += 1;
                    count += await cleanIndex(idx);
                }
            }
        }

        console.log('INFO: ' + count + ' out of ' + total + ' indexes were removed');
    }
    else {
        console.log('INFO: Storage is fine, no index will be deleted');
    }
}

function cleanIndex(idx) {
    console.log('INFO: Cleaning index ' + idx);
    console.log('INFO: URL=https://' + parameters.endpoint + '/' + idx);
    var requestParams = buildRequest(
        parameters.endpoint,
        idx,
        'DELETE',
        null,
        null
    );

    return new Promise(resolve => {
        client.handleRequest(requestParams, null, function (response) {
            var responseBody = '';
            response.on('data', function (chunk) {
                responseBody += chunk;
            });
            response.on('end', function () {
                var info = JSON.parse(responseBody);

                if (response.statusCode == 200) {
                    resolve(1);
                }

                var error = response.statusCode != 200 || info.Message != null ? {
                    "statusCode": response.statusCode,
                    "responseBody": responseBody
                } : null;

                if (error != null) {
                    console.log(JSON.stringify(error, null, 2));
                    resolve(0);
                }
            });
        }, function (error) {
            console.log(JSON.stringify(error, null, 2));
            resolve(0);
        });
    });
}

////////////////////////////////////////////////////////////////
// AWS Authentication Signature V4 Part
////////////////////////////////////////////////////////////////

function buildRequest(endpoint, path, method, query, body) {
    var endpointParts = endpoint.match(/^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/);
    //console.log(JSON.stringify(endpointParts, null, 2));
    var region = endpointParts[2];

    if (path == null) path = '';
    if (query == null) query = '';
    var tmpQuery = (query == '') ? '' : '?' + query;
    if (body == null) body = '';

    var request = new AWS.HttpRequest(endpoint, region);

    request.method = method;
    request.path += path + tmpQuery;
    request.body = body;
    request.headers['host'] = endpoint;
    request.headers['Content-Type'] = 'application/json';

    var credentials = new AWS.EnvironmentCredentials('AWS');
    var signer = new AWS.Signers.V4(request, 'es');
    signer.addAuthorization(credentials, new Date());

    //console.log(JSON.stringify(request, null, 2));
    return request;
}

////////////////////////////////////////////////////////////////
// Helper Function Part
////////////////////////////////////////////////////////////////

function getDiffDateTimestamp(year, month, day, yearDiff, monthDiff, dayDiff) {
    year = parseInt(year);
    month = parseInt(month);
    day = parseInt(day);
    var date = new Date();
    date.setFullYear(year - yearDiff, month - 1 - monthDiff, day - dayDiff);
    date.setHours(0, 0, 0, 0);
    return date.getTime();
}