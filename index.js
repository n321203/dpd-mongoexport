var Resource = require('deployd/lib/resource')
var util = require('util');
var internalClient = require('deployd/lib/internal-client')
var spawn = require('child_process').spawn;

var DPD_MONGOEXPORT_KEY = process.env.DPD_MONGOEXPORT_KEY;
var mongodbUri = require('mongodb-uri');

function MongoExport(name, options) {
    Resource.apply(this, arguments);
}

util.inherits(MongoExport, Resource);
module.exports = MongoExport;

MongoExport.prototype.clientGeneration = true;
MongoExport.prototype.handle = function (ctx, next) {

    var uriObject           = mongodbUri.parse(process.server.db.connectionString);
    var DB_USERNAME         = uriObject.username;
    var DB_PASS             = uriObject.password;
    var DB_NAME             = uriObject.database;

    var dpd = internalClient.build(process.server);

    // Validate
    var body = ctx.req.body || {}
    if(!body || !body.k || body.k != DPD_MONGOEXPORT_KEY){
        return next();
    }

    // Export fields:
    var fields = "username,loans.0.firstName,loans.0.lastName,_id,token,loans.0.loan,loans.0.address,loans.0.zip,loans.0.city,loans.0.municipality,loans.0.phone,loans.0.income,loans.0.bank,created,loans.0.created"
    fields += ",hasAcceptedOffer,hasDeclinedOffer"
    fields += ",hasAcceptedOffer201411,hasDeclinedOffer201411"
    fields += ",hasAcceptedOfferSthlm201412,hasDeclinedOfferSthlm201412"
    fields += ",hasAcceptedOfferRekarne,hasDeclinedOfferRekarne"
    fields += ",hasAcceptedOfferSEBJagersro,hasDeclinedOfferSEBJagersro"
    fields += ",hasAcceptedOfferSHBMalmo,hasDeclinedOfferSHBMalmo"
    fields += ",hasAcceptedOfferLFSkane,hasDeclinedOfferLFSkane"
    fields += ",loans.0.name,declineReason,declineReason201411,declineReasonSthlm201412"
    fields += ",hasShared,referralId,referredById,notes,isAdmin,name,socialAccount"

    // From & to
    var createdFrom     = (body.createdFrom) ? body.createdFrom : "2010-01-01"
    var createdTo       = (body.createdTo) ? body.createdTo : "2020-01-01";
    var loanFrom        = (body.loanFrom) ? body.loanFrom : "2010-01-02";
    var loanTo          = (body.loanTo) ? body.loanTo : "2020-01-01";

    if(loanFrom && loanFrom != "2010-01-02"){
        var dbquery     = '{ loans: { $elemMatch: { created: {$gte: "' + loanFrom + '"}, created: {$lte:"' + loanTo + '"} } } }' 
    }
    else 
        var dbquery     = "{ created: {$gte: \"" + createdFrom + "\"}, created:{$lte: \"" + createdTo + "\"} }"

   var params = [ 
        '--host', 'paulo.mongohq.com',
        '--port', 10006,
        '--db', DB_NAME,
        '--username', DB_USERNAME,
        '--password', DB_PASS]
    params.push('--collection', 'users', "--fields", fields, "--query", dbquery, "--csv")
    //params.push('--query', '{ loans: { $elemMatch: { created: {$gte: "2014-12-01"}, created: {$lte:"2015-01-01"} } } }')
    var mongoExport = spawn('mongoexport', params)

    // Results:
    var result = "";
    mongoExport.stdout.on('data', function(data) {
        result += data;
    });
    mongoExport.on('close', function(code) {      
        // Replace all commas within quotes (in comments etc) with something else
        result = result.replace(/".*?"/g, function(str) {
            return str.replace(/,/g, '±');
        });
        // Now change all commas to semi-colons
        result = result.replace(/,/g, ";")

        // Replace back commas within quotes
        result = result.replace(/".*?"/g, function (str) {
            return str.replace(/±/g, ',');
        })

        result = "<meta charset='utf-8'><pre>" + result + "</pre>"
        ctx.done(null, result)
    })
}
