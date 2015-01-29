var http = require('http'),
    fs = require('fs'),
    follow = require('follow'),
    mkpath = require('mkpath'),
    Duration = require('duration'),
    async = require('async'),
    localhost = "http://ole:oleoleole@127.0.0.1:5984",
    oledemo = "https://oledemo:oleoleole@oledemo.cloudant.com",
    olesomalia = "https://olesomalia:oleoleole@olesomalia.cloudant.com",
    openBell = "http://openbell.ole.org:5984",
    bebo = "http://pi:raspberry@192.168.0.100:5984",
    walk = require('walk'),
    request = require('request'),
    follow_context,
    counter = 0,
    arrProblematicResources = [],
    pdfSize,
    temp_conversion_folder_name = "temp_conversion";
    temp_conversion_folder_path = temp_conversion_folder_name;

// vars for duration logging
var dateDownloadStart, dateDownloadEnd, dateConversionStart, dateConversionEnd, dateUploadStart, dateUploadEnd;

var current_target_couch_server = process.argv[2];
if (!process.argv[2]) return console.log('No CouchDB URL provided. Quiting now.');

var nano = require('nano')(current_target_couch_server), resources = nano.use('resources');

follow({db: current_target_couch_server + "/resources",since:'now'}, function(error, change) {
    follow_context = this;
    if(!error) {
        console.log("Event number " + (++counter));
        var revId = change.changes[0].rev;
        var resId = change.id;
        follow_context.pause();
        getData();
    } else {
        console.log(error);
    }
});

console.log("PDF optimization engine is up and running..");
    

function  getData(){
    var resId = "";
    resources.view('bell', 'check_for_optimization', {skip: 0, limit: 50}, function(err, body) {
        if (!err) {
             console.log("Number of PDF resources to process: " + body.rows.length);
             var resourcesRemainingCount = body.rows.length;
             async.eachSeries(body.rows, function (doc, callback) {
                    console.log("Resources remaining count: " + (resourcesRemainingCount--));
                    var attac = doc.value._attachments
                    resId = doc.value._id; // resId = document _id
                    if(attac) {
                        var keys = Object.keys(attac);
                        pdfSize = Math.floor(attac[keys[0]].length)/1000; // size in KB's
                        console.log("Resource name: " + keys[0] + " size: " + pdfSize + " KB, id: " + resId);

                        // if not marked as couldNotBeProcessed, then proceed to download it otherwise proceed to next 
                        // resource's iteration
                        if (arrProblematicResources.indexOf(resId) === -1) {
                                downloadFile(resId, keys[0], callback);
                        } else {
                            console.log("Resource " + keys[0] + ", id " + resId + " is problematic, proceeding to " + 
                                "process next resource..");
                            callback();
                        }
                    } else {
                        console.log('Resource with id ' + resId +' has no attachments');
                        callback();
                    }
                }, function (err) {
                    if (err) {
                        console.log("Exiting the outer view_result processing loop");
                        if(resId){
                            resources.get(resId, function(getErr, body) {
                                if (!getErr){
                                    resources.insert(body, resId, function (insertError, response) {
                                        if(!insertError) {
                                            console.log("Successfully generated a dummy event from error handling block to trigger the service again");
                                        } else {
                                            console.log("Error inserting doc while trying to generate dummy event to retrigger the service");
                                        }
                                    });
                                } else {
                                    console.log("error in trying to generate a dummy event for triggering service again.");
                                }
                            });
                            // add this document's id in the "couldNotBeProcessed.txt"
                            console.log("Marking resource with id " + resId + " as problematic");
                            arrProblematicResources.push(resId);
                            var filename_couldNotBeProcessed = "couldNotBeProcessed.txt";
                            writeToFile(filename_couldNotBeProcessed, "" + resId + "\n", "id of resource written to " + filename_couldNotBeProcessed +
                                " after its processing aborted with error");
                        } else {
                            console.log("doc id could not be added to the couldNotBeProcessed.txt file as the id was null");
                        }
                    }else{
                        console.log("All documents returned by 'check_for_optimization' view have been processed");
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
////=================================================================
function downloadFile(resId, fileName, callback) {
    console.log("Downloading resource...");
    dateDownloadStart = new Date();
    // console.log("Downloading duration clock started");
    resources.attachment.get(resId, encodeURIComponent(fileName), function (err, body) {
        if (!err) {
            mkpath(temp_conversion_folder_name + '/' + resId, function (err) {
                if (err){
                    console.log("error creating folder: " + temp_conversion_folder_name + '/' + resId);
                    callback(err);
                }
//                console.log('Folder ' + temp_conversion_folder_name + '/'+resId+ ' created for processing the pdf ' + fileName);
                fs.writeFile(temp_conversion_folder_name + '/' + resId + '/' +fileName, body, function (err) {
                    if (err) {
                        console.log("downloadFile:: Error In writing file");
                        console.log(err);
                        callback(err);
                    } else {
                        dateDownloadEnd = new Date();
                        console.log("Resource successfully downloaded");
                        // console.log("Downloading duration clock stopped: " + + (new Duration(dateDownloadStart, dateDownloadEnd)).seconds + " seconds");
                        var exec = require('child_process').exec;
                        var sourcePdfPath = fileName;
                        var destPdfPath = temp_conversion_folder_path + "\\" + resId;
                        console.log("Starting conversion of the pdf resource into imagebook...");
                        dateConversionStart = new Date();
                        // console.log("Conversion duration clock started");
                        var child = exec('pdfToImageConvertor.bat "'+sourcePdfPath+'" "'+destPdfPath+'"',
                            function( error, stdout, stderr) {
                                if ( error == null ) {
                                    dateConversionEnd = new Date();
                                    console.log("Resource successfully converted from pdf to images");
                                    // console.log("Conversion duration clock stopped: " + (new Duration(dateConversionStart, dateConversionEnd)).seconds + " seconds");
                                    uploadFiles(resId, fileName, callback);
                                } else {
                                    console.log(error);
                                    console.log("error executing pdf-to-images conversion batch file");
                                    callback(error);
                                }
                            }
                        );
                    }
                });
            });
        }
        else {
            callback(err);
        }
    });
}
//======================= upload =====================
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
                console.log("Uploading imagebook of the resource..");
                numOfPages = files.length;
                console.log(numOfPages + " files");
                // set need_optimization to false and set openWthi to "Bell-Reader"
                body.need_optimization = false;
                body.openWith = "Bell-Reader";
                dateUploadStart = new Date();
                // console.log("Uploading duration clock started");
                resources.multipart.insert(body,files,resId, function(err, body) {
                    dateUploadEnd = new Date();
                    console.log("Imagebook successfully uploaded");
                    // console.log("Uploading duration clock stopped: " + (new Duration(dateUploadStart, dateUploadEnd)).seconds + " seconds");
                    // log durations for downloading, conversion and uploading for this resource

                   var durationDownload = new Duration(dateDownloadStart, dateDownloadEnd);
                   var durationConversion = new Duration(dateConversionStart, dateConversionEnd);
                   var durationUpload = new Duration(dateUploadStart, dateUploadEnd);
                   var resourceDurationLogEntry = "" + resId + "   " + "size: " + pdfSize + ",  "  + "pages: " + numOfPages +
                       ",  downloading: (" + durationDownload.minutes + ", " + durationDownload.second + ")" +
                       ",  conversion: (" + durationConversion.minutes + ", " + durationConversion.second + ")" + ",  " +
                       "uploading: (" + durationUpload.minutes + ", " + durationUpload.second + ")" +
                       ",  finshing_time: (" + dateUploadEnd.toString() + ")" +
                       ",   resource: " + fileName + "\n" ;
                    var strUrl = "http://openbell.ole.org:5984/apps/_design/bell/bell-resource-router/index.html#open/" +
                                                resId + "\n";
                    writeToFile("imagebookURLs.txt", strUrl, "Resource URL written to imagebookURLs.txt after being completely processed");
                    writeToFile("Durations.txt", resourceDurationLogEntry, "Processing durations for the resource logged to Durations.txt");

                    // garbage collect the folder whose contents (images of the source PDF) have been uploaded successfully
                    var exec = require('child_process').exec;
                    var destPdfPath = temp_conversion_folder_path + "\\" + resId;
                    var child = exec('deleteFolder.bat "' + destPdfPath + '"' , function( error, stdout, stderr) {
                        if ( error == null ) {
//                            console.log("Temporary folder for processing resource (" + fileName +
//                                ") successfully garbage collected");
                        } else{
                            console.log("Error executing deleteFolder.bat");
                        }
                    });
                    mainCallback();
                });
            } else {
                console.log('uploadFiles:: error while fetching the document');
                console.log(err);
                mainCallback(err);
            }
        });
    });
}