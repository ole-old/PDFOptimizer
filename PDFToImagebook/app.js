if (!process.argv[2]) return console.log('No CouchDB URL provided. Quiting now.');

//var request = require('request');
var fs = require('fs-extra'), rimraf = require('rimraf'),  Duration = require('duration'),  async = require('async'),
    walk = require('walk'),  follow_context,  arrProblematicResources = [],  pdfSize,  temp_conversion_folder_name = "temp_conversion",
    temp_conversion_folder_path = temp_conversion_folder_name;
// vars for duration logging
var dateDownloadStart, dateDownloadEnd, dateConversionStart, dateConversionEnd, dateUploadStart, dateUploadEnd;

var current_target_couch_server = process.argv[2];

var nano = require('nano')(current_target_couch_server);
var resources = nano.use('resources');

function writeToFile(fileName, data, successMsg) {
    fs.open("./" + temp_conversion_folder_name + "/" + fileName, 'a', 0666, function(err, fd){
        fs.write(fd, data, null, undefined, function (err, written) {
            if(!err){
//                console.log(successMsg);
            } else {
                console.log("error writing about the resource in " + fileName + " file");
            }
        });
    });
}

function uploadFiles(resId, fileName, mainCallback){
    var files   = [];
    var numOfPages = 0;
    // Walker options
    var folderName = "./" + temp_conversion_folder_name + "/" + resId;
    var walker  = walk.walk(folderName, { followLinks: false });
    walker.on('file', function(root, stat, next) {
        var fName = root + '/' + stat.name;
        fs.readFile(fName, function(err, data) {
            if(!err) {
                if (fName.indexOf(".pdf") > -1) {
                    files.push({name: stat.name, data: data, content_type: 'application/pdf'});
                } else {
                    files.push({name: stat.name, data: data, content_type: 'image/jpeg'});
                }
            } else {
                console.log("Error reading file " + fName);
                mainCallback(err);
            }
        });
        next();
    });
    walker.on('end', function() {
        resources.get(resId, function(err, body) {
            if (!err){
                console.log("Uploading imagebook for the PDF resource..");
                numOfPages = files.length - 1;
                console.log(numOfPages + "+1" + " files");
                // set need_optimization to false and set openWthi to "Bell-Reader"
                body.need_optimization = false;
                body.openWith = "Bell-Reader";
                dateUploadStart = new Date();
                // console.log("Uploading duration clock started");
                resources.multipart.insert(body, files, resId, function(err, body) {
                    if (err) {
                        mainCallback(err);
                        return;
                    }
                    dateUploadEnd = new Date();
                    console.log("Imagebook successfully uploaded");
                    // console.log("Uploading duration clock stopped: " + (new Duration(dateUploadStart, dateUploadEnd)).seconds + " seconds");
                    // log durations for downloading, conversion and uploading for this resource
                    var durationDownload = new Duration(dateDownloadStart, dateDownloadEnd);
                    var durationConversion = new Duration(dateConversionStart, dateConversionEnd);
                    var durationUpload = new Duration(dateUploadStart, dateUploadEnd);
                    var resourceDurationLogEntry = "doc_id:" + resId + "   " + "size: " + pdfSize + ",  "  + "pages: " + numOfPages +
                        ",  downloading: (" + durationDownload.minutes + ", " + durationDownload.second + ")" +
                        ",  conversion: (" + durationConversion.minutes + ", " + durationConversion.second + ")" + ",  " +
                        "uploading: (" + durationUpload.minutes + ", " + durationUpload.second + ")" +
                        ",  finshing_time: (" + dateUploadEnd.toString() + ")" +
                        ",   pdf_title: " + fileName + "\n" ;
                    var strUrl = current_target_couch_server + "/apps/_design/bell/bell-resource-router/index.html#open/" + resId + "\n";
                    writeToFile("imagebookURLs.txt", strUrl, "Resource URL written to imagebookURLs.txt after being completely processed");
                    writeToFile("Durations.txt", resourceDurationLogEntry, "Processing durations for the resource logged to Durations.txt");
                    // garbage collect the folder whose contents (images of the source PDF) have been uploaded successfully
                    rimraf(temp_conversion_folder_name + '/' + resId, function (err) {
                        if (err) {
                            callback(err);
                        }
                        console.log(temp_conversion_folder_name + '/' + resId + ' folder deleted after successful upload');
                        console.log('moving on to the next resource document');
                        mainCallback();
                    });
                });
            } else {
                console.log('uploadFiles:: error while fetching the document');
                mainCallback(err);
            }
        });
    });
}

function downloadFile(resId, fileName, callback) {
//    console.log("Downloading resource...");
    dateDownloadStart = new Date();
    // console.log("Downloading duration clock started");
    resources.attachment.get(resId, encodeURIComponent(fileName), function (err, body) {
        if (err) {
            callback(err);
            return;
        }
        // write file at the location temp_conversion_folder_name/resourceDocId
        fs.outputFile(temp_conversion_folder_name + '/' + resId + '/' + fileName, body, function (err) {
            if (err) {
                callback(err);
            }
            dateDownloadEnd = new Date();
            console.log("Resource successfully downloaded");
            // console.log("Downloading duration clock stopped: " + + (new Duration(dateDownloadStart, dateDownloadEnd)).seconds + " seconds");
            var exec = require('child_process').exec;
            var sourcePdfPath = fileName;
            var destImagebookPath = temp_conversion_folder_name + "\\" + resId;
//            console.log("Starting conversion of the pdf resource into imagebook...");
            dateConversionStart = new Date();
            // console.log("Conversion duration clock started");
            var child = exec('pdfToImageConvertor.bat "'+sourcePdfPath+'" "'+destImagebookPath+'"',
                function( error, stdout, stderr) {
                    if ( error == null ) {
                        dateConversionEnd = new Date();
                        console.log("Resource successfully converted from pdf to images");
                        // console.log("Conversion duration clock stopped: " + (new Duration(dateConversionStart, dateConversionEnd)).seconds + " seconds");
                        uploadFiles(resId, fileName, callback);
                    } else {
                        console.log("error executing pdf-to-images conversion batch script");
                        callback(error);
                    }
                }
            );
        });
    });
}

function createDummyEventToRetriggerService (resourceDocId) {
    resources.get(resourceDocId, function(getErr, body) {
        if (!getErr){
            resources.insert(body, resourceDocId, function (insertError, response) {
                if(!insertError) {
                    console.log("Successfully generated a dummy event to trigger the service again");
                } else {
                    console.log("Error while trying to generate dummy event to retrigger the service");
                }
            });
        } else {
            console.log("error in trying to generate a dummy event for triggering service again.");
        }
    });
}

function  getData(){
    var resId = "", limit = 20;
    resources.view('bell', 'check_for_optimization', {skip: 0, limit: limit}, function(err, body) {
        if (!err) {
             console.log("Number of PDF resources to process: " + body.rows.length);
             var resourcesRemainingCount = body.rows.length;
             async.eachSeries(body.rows, function (doc, callback) {
                    console.log("Resources remaining count: " + (resourcesRemainingCount--));
                    var attac = doc.value._attachments
                    resId = doc.value._id; // resId = document _id
                    if(attac) {// if true then am assuming attachments must have atleast one key in it i-e (Object.keys(attac).length) > 0
                        var keys = Object.keys(attac); // keys is, hopefully, an array of names of files attached to a resource document
                        var attachmtsCount = keys.length, tokens = keys[0].split(".");
                        var fileExt = tokens[tokens.length - 1];
                        if ( (attachmtsCount === 1) && (fileExt === 'pdf') ) { // pass this on for conversion to imagebook
                            pdfSize = Math.floor(attac[keys[0]].length)/1000; // size in KB's
                            console.log("Resource name: " + keys[0] + ", size: " + pdfSize + " KB, id: " + resId);
                            // if not marked as couldNotBeProcessed, then proceed to download it otherwise proceed to next
                            // resource's iteration
                            if (arrProblematicResources.indexOf(resId) === -1) {
                                downloadFile(resId, keys[0], callback);
                            } else {
                                console.log("Resource " + keys[0] + ", id " + resId + " is problematic");
                                console.log('moving on to the next resource document');
                                callback();
                            }
                        } else {
                            console.log('Resource with id ' + resId +' has either more than 1 attachment(s) or has a non-pdf attachment');
                            console.log('moving on to the next resource document');
                            callback();
                        }
                    } else {
                        console.log('Resource with id ' + resId +' has no attachments');
                        console.log('moving on to the next resource document');
                        callback();
                    }
                }, function (err) {
                     if (err) {
                         if (err.reason && err.reason === 'function_clause') { // couch response error
                             console.log("couchdb threw an error"); console.log(err);
                             if(resId) {
                                 createDummyEventToRetriggerService(resId);
                             }
                         } else if (err.code && err.code === 'ECONNRESET') { // connection error
                             console.log(err);
                             if(resId) {
                                 createDummyEventToRetriggerService(resId);
                             }
                         } else {
                             console.log(err);
                             console.log("Exiting the current round of processing pdf resources with error");
                             if(resId){
                                 createDummyEventToRetriggerService(resId);
                                 arrProblematicResources.push(resId);
                                 console.log("Marked resource with id " + resId + " as problematic");
                                 // add this document's id in the "couldNotBeProcessed.txt"
                                 var filename_couldNotBeProcessed = "couldNotBeProcessed.txt";
                                 writeToFile(filename_couldNotBeProcessed, "" + resId + "\n", "id of resource written to " + filename_couldNotBeProcessed +
                                     " after its processing aborted with error");
                             } else {
                                 console.log("doc id could not be added to the couldNotBeProcessed.txt file as the id was null");
                             }
                         }
                     }else{
                         console.log("Exiting the current round of processing pdf resources with success!!");
                     }
                     follow_context.resume();
                }
             );
        } else {
            console.log(err);
            follow_context.resume();
        }
    });
}

var feed = resources.follow({since: "now"});
feed.on('change', function (change) {
    follow_context = this;
//    console.log(change);
    follow_context.pause();
    getData();
});
feed.follow();