var assign = require('object-assign');
var nameFunctions = require('keystone-storage-namefunctions');
var ensureCallback = require('keystone-storage-namefunctions/ensureCallback');
var pathlib = require('path');
var s3 = require('s3');
var AWS = require('aws-sdk');
var fs = require('fs');

var DEFAULT_OPTIONS = {
    key: process.env.S3_KEY,
    secret: process.env.S3_SECRET,
    bucket: process.env.S3_BUCKET,
    region: process.env.S3_REGION || 'us-east-1',
    generateFilename: nameFunctions.randomFilename,
};

function S3Adapter(options, schema) {
    this.options = assign({}, DEFAULT_OPTIONS, options.s3);

    this.client = s3.createClient({
        maxAsyncS3: 20,     // this is the default 
        s3RetryCount: 3,    // this is the default 
        s3RetryDelay: 100, // this is the default 
        multipartUploadThreshold: 20971520, // this is the default (20 MB) 
        multipartUploadSize: 15728640, // this is the default (15 MB),
        s3Options: {
            accessKeyId: this.options.key,
			secretAccessKey: this.options.secret,
			signatureVersion: 'v4',
			region: this.options.region
        }
    })


    // Support `defaultHeaders` option alias for `headers`
    // TODO: Remove me with the next major version bump
    if (this.options.defaultHeaders) {
        this.options.headers = this.options.defaultHeaders;
    }

    // If path is specified it must be absolute.
    if (options.path != null && !pathlib.isAbsolute(options.path)) {
        throw Error('Configuration error: S3 path must be absolute');
    }

    // Ensure the generateFilename option takes a callback
    this.options.generateFilename = ensureCallback(this.options.generateFilename);
}

S3Adapter.compatibilityLevel = 1;

// All the extra schema fields supported by this adapter.
S3Adapter.SCHEMA_TYPES = {
    filename: String,
    bucket: String,
    path: String,
    etag: String,
};

S3Adapter.SCHEMA_FIELD_DEFAULTS = {
    filename: true,
    bucket: false,
    path: false,
    etag: false,
};

S3Adapter.prototype.uploadFile = function (file, callback) {
    var self = this;
    this.options.generateFilename(file, 0, function (err, filename) {
        if (err) return callback(err);

        // The expanded path of the file on the filesystem.
        var localpath = file.path;

        file.path = self.options.path[0] == '/' ? self.options.path.substr(1, self.options.path.length) : self.options.path;
        file.filename = filename;

        // Figure out headers
        var headers = assign({}, self.options.headers, {
            'Content-Length': file.size,
            'Content-Type': file.mimetype,
        });

        var uploader = self.client.uploadFile({
            localFile: localpath,

            s3Params: {
                Bucket: self.options.bucket,
                Key: file.path + '/' + filename,
                ACL: 'public-read',
            },
        });
        uploader.on('error', function (err) {
            console.error("unable to upload:", err.stack);
            callback(new Error(err.stack))
        });

        //Progress Of Upload - Can Be Logged

        // uploader.on('progress', function () {
        //     console.log("progress", uploader.progressMd5Amount,
        //         uploader.progressAmount, uploader.progressTotal);
        // });

        uploader.on('end', function () {
            file.key = file.path + '/' + filename
            callback(null, file);
        });
    });
};

S3Adapter.prototype.getFileURL = function (file) {
    return s3.getPublicUrl(this.options.bucket, file.key, this.options.region)
};

module.exports = S3Adapter;
