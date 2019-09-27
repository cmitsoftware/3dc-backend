'use strict'

const AWS = require('aws-sdk')
var ObjectId = require('mongodb').ObjectID;

var MongoClient = require('mongodb').MongoClient;

let atlas_connection_uri;
let cachedDb = null;

exports.handler = (event, context, callback) => {
    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'];

    console.log(event);
    
    if (atlas_connection_uri != null) {
        processEvent(event, context, callback);
    } 
    else {

        if(event.localTest == null)  {
            const kms = new AWS.KMS();
            
            kms.decrypt({ CiphertextBlob: new Buffer(uri, 'base64') }, (err, data) => {
                if (err) {
                    console.log('Decrypt error:', err);
                    return callback(err);
                }
                atlas_connection_uri = data.Plaintext.toString('ascii');
                //console.log('the Atlas connection string is ' + atlas_connection_uri);
                processEvent(event, context, callback);
            }); 
        } else {
            atlas_connection_uri = uri;
            processEvent(event, context, callback);
        }
    } 
};

function processEvent(event, context, callback) {
    //var jsonContents = JSON.parse(JSON.stringify(event));
    console.log('Processing method: ' + event.httpMethod);

    //the following line is critical for performance reasons 
    //to allow re-use of database connections across calls to this Lambda function 
    //and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, 
    // while subsequent, close calls will only take a few hundred milliseconds.
    context.callbackWaitsForEmptyEventLoop = false;

    if(event.httpMethod == 'GET') {
        handleGet(event, context, callback);
    } else if(event.httpMethod == 'POST') {
        handlePost(event, context, callback);
    } else if(event.httpMethod == 'DELETE') {
        handleDelete(event, context, callback);
    } else if(event.httpMethod == 'PUT') {
        handlePut(event, context, callback);
    } else {
        callback(null, {statusCode:500, body:JSON.stringify({result: "Method not implemented"})});
    }
}

function handlePost(event, context, callback) {
    
    /*
    if(event.body.subscriptionDate != null) {
        event.body.subscriptionDate = new Date(event.body.subscriptionDate);
    }
    */

    try {
        if (cachedDb == null) {
            console.log('=> connecting to database');
            
            MongoClient.connect(atlas_connection_uri, {useNewUrlParser: true, useUnifiedTopology: true }, function (err, client) {
                if (err) {
                    console.error('An error occurred connecting to MongoDB: ', err);
                } else {
                    cachedDb = client.db('3dc');
                    createPerson(cachedDb, event.body, callback);
                }
            });
        }
        else {
            createPerson(cachedDb, event.body, callback);
        }
    } 
    catch (err) {
        console.error('an error occurred', err);
        callback(null, {statusCode: 500, body: JSON.stringify({result:'An error occurred'})});
    }
}

function handleGet(event, context, callback) {
    
    if(event.pathParameters != null && event.pathParameters.personId != null) {
        console.log(event.pathParameters);    
        try {
            if (cachedDb == null) {
                
                MongoClient.connect(atlas_connection_uri, {useNewUrlParser: true, useUnifiedTopology: true }, function (err, client) {
                    console.log('=> connecting to database');
                    if (err) {
                        console.error('An error occurred connecting to MongoDB: ', err);
                    } else {
                        cachedDb = client.db('3dc');
                        console.log("cached db set");
                        getPerson(cachedDb, event.pathParameters.personId, callback);
                    }
                });
            }
            else {
                console.log("cached db already available");
                getPerson(cachedDb, event.pathParameters.personId, callback);
            }
        } 
        catch (err) {
            console.error('an error occurred', err);
            callback(null, {statusCode: 500, body: JSON.stringify({result:'An error occurred'})});
        }
    } else if(event.queryStringParameters != null && event.queryStringParameters.filter != null) {
        console.log(event.queryStringParameters);
        try {
            if (cachedDb == null) {
                console.log('=> connecting to database');
                
                MongoClient.connect(atlas_connection_uri, {useNewUrlParser: true, useUnifiedTopology: true }, function (err, client) {
                    if (err) {
                        console.error('An error occurred connecting to MongoDB: ', err);
                    } else {
                        cachedDb = client.db('3dc');
                        filterPerson(cachedDb, event.queryStringParameters.filter, callback);
                    }
                });
            }
            else {
                filterPerson(cachedDb, event.queryStringParameters.filter, callback);
            }
        } 
        catch (err) {
            console.error('an error occurred', err);
        }
    } else {
        console.log("Input not valid");
    }
}

function handleDelete(event, context, callback) {
    
    if(event.pathParameters != null && event.pathParameters.personId != null) {
        
        console.log(event.pathParameters);    
        
        try {
            if (cachedDb == null) {
                console.log('=> connecting to database');
                
                MongoClient.connect(atlas_connection_uri, {useNewUrlParser: true, useUnifiedTopology: true }, function (err, client) {
                    if (err) {
                        console.error('An error occurred connecting to MongoDB: ', err);
                    } else {
                        cachedDb = client.db('3dc');
                        deletePerson(cachedDb, event.pathParameters.personId, callback);
                    }
                });
            }
            else {
                deletePerson(cachedDb, event.pathParameters.personId, callback);
            }
        } 
        catch (err) {
            console.error('an error occurred', err);
            callback(null, {statusCode: 500, body: JSON.stringify({result:'An error occurred'})});
        }

    } else {
        console.log("Input not valid");
    }
}

function handlePut(event, context, callback) {
    
    try {
        if (cachedDb == null) {
            console.log('=> connecting to database');
            
            MongoClient.connect(atlas_connection_uri, {useNewUrlParser: true, useUnifiedTopology: true }, function (err, client) {
                if (err) {
                    console.error('An error occurred connecting to MongoDB: ', err);
                } else {
                    cachedDb = client.db('3dc');
                    updatePerson(cachedDb, event.pathParameters.personId, event.body, callback);
                }
            });
        }
        else {
            updatePerson(cachedDb, event.pathParameters.personId, event.body, callback);
        }
    } 
    catch (err) {
        console.error('an error occurred', err);
        callback(null, {statusCode: 500, body: JSON.stringify({result:'An error occurred'})});
    }
}

function createPerson(db, json, callback) {

    db.collection('persons').insertOne( JSON.parse(json), function(err, result) {
        if(err!=null) {
            console.error("An error occurred in createPerson", err);
            callback(null, {statusCode:500, body:JSON.stringify(err)});
        }
        else {
            console.log("Person created with id: " + result.insertedId);
            let returnValue = {
                statusCode: 201, 
                body: JSON.stringify({personId:result.insertedId}),
                headers: {},
                isBase64Encoded: false
            }
            callback(null, {statusCode:200, body:JSON.stringify({result: result})});
        }
        //we don't need to close the connection thanks to context.callbackWaitsForEmptyEventLoop = false (above)
        //this will let our function re-use the connection on the next called (if it can re-use the same Lambda container)
        //db.close();
    });
};

function getPerson(db, personId, callback) {
    console.log("Getting person " + personId);
    let objectId = new ObjectId(personId);

    db.collection("persons").findOne({_id: objectId}, function(err, person) {
        if (err)  {
            console.error("An error occurred getPerson", err);
            callback(null, {statusCode:500, body:JSON.stringify(err)});
        }
        if(person == null) {
            callback(null, {statusCode:404, body:null});
        }
        console.log("Person found: " + person);
        callback(null, {statusCode:200, body:JSON.stringify(person)});
    });
}

function filterPerson(db, filterString, callback) {
    console.log("Filtering person " + filterString);
    let filter = JSON.parse(filterString);
    //let filter = eval(filterString);
    console.log("filter: " + filter);
    let orFilter = [];
    if (filter != null) {
        filter.forEach(function(value, key, map){
            console.log(value);
            let orClause = new Object();
            //let orClauseValue = new Object();
            orClause[key] = {'$regex': value, '$options': 'i'};
            orFilter.push(orClause);
        });
    }
    /*
    db.collection("persons").find(
        {'$or':[
            {'name': {'$regex': filter, '$options': 'i'}},
            {'surname': {'$regex': filter, '$options': 'i'}}
        ]}
    )
    */
   console.log(orFIlter);
    db.collection("persons").find(
        {'$or': orFilter}
    )
    .toArray(function(err, persons) {
        if (err) {
			console.error("An error occurred getPerson", err);
            callback(null, {statusCode:500, body:JSON.stringify(err)});
		}
        console.log("Persons found: " + persons); 
        callback(null, {statusCode:200, body:JSON.stringify(persons)});
    });
}

function deletePerson(db, personId, callback) {
    console.log("Deleting person " + personId);
    let objectId = new ObjectId(personId);

    db.collection("persons").deleteOne({_id: objectId}, function(err, result) {
        if (err) throw err;
        console.log("Deleted records: " + result.deletedCount);
        callback(null, {statusCode:200, body:null});
    });
}

function updatePerson(db, personId, body, callback) {
    console.log("Updating person " + personId);
    let objectId = new ObjectId(personId);

    db.collection("persons").updateOne({_id: objectId}, {$set: JSON.parse(body)}, function(err, result) {
        if(err!=null) {
            console.error("An error occurred in updatePerson", err);
            callback(null, {statusCode:500, body:JSON.stringify(err)});
        }
        console.log("Result: " + result);
        getPerson(cachedDb, personId, callback);
        //callback(null, {statusCode:200, body:result});
    });
}