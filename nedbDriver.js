'use strict';

var nedb = require('nedb'),
	when = require('when'),
	fs = require('fs')
;

var NedbDriver = function(options){
		this.dataPath = options && options.dataPath ? options.dataPath : __dirname + '/data';
		console.log('NeDB initialized at ' + this.dataPath);
	},
	collections = {},

	// Fortunatelly neDB gives the API needed out of the box.
	TuleCollection = function(collectionName, dataPath){
		var me = this,
			deferred = when.defer(),
			collection = collections[collectionName]
		;

		this.collectionName = collectionName;

		if(collection)
			this.db = collection;
		else{
			this.db = deferred.promise;

			// We store the nedb in disk
			collection = new nedb({
				filename: dataPath + '/' + collectionName + '.db',
				autoload: true,
				onload: function(err){
					if(err){
						console.log(err);
						deferred.reject(err);
					}
					else{
						console.log('neDb collection ' + collectionName + ' loaded.');
						me.db = collection;
						collections[collectionName] = collection;
						deferred.resolve(collection);
					}
				}
			});
		}
	}
;

/**
 * The driver will complete the API as described in
 * http://tulecms.arqex.com/db-driver-api/
 *
 * As long as this driver always store the data in disk, most of the
 * api create and delete archives to work, so there is a hard dependency
 * on fs module.
 *
 */
NedbDriver.prototype = {
	collection: function(collectionName){
		return new TuleCollection(collectionName, this.dataPath);
	},
	createCollection: function(collectionName, callback){
		var me = this;

		fs.open(this.dataPath + '/' + collectionName + '.db', 'a', function(err, fd){
			if(err)
				return callback(err);

			fs.close(fd, function(err){
				callback(err, new TuleCollection(collectionName, me.dataPath));
			});
		});
	},
	getCollectionNames: function(callback){
		var collectionNames = [];

		// The only way of having the collection names is looking for different .db files
		// in our data path.
		fs.readdir(this.dataPath, function(err, files){
			if(err)
				return callback(err);

			files.forEach(function(file){
				var extensionIndex = file.length - 3;
				if(file.length > 3 && file.substring(extensionIndex) == '.db')
					collectionNames.push(file.substring(0, extensionIndex));
			});

			callback(null, collectionNames);
		});
	},
	renameCollection: function(oldName, newName, callback){
		var oldPath = this.dataPath + '/' + oldName + '.db',
			newPath = this.dataPath + '/' + newName + '.db'
		;

		fs.rename(oldPath, newPath, function(err){
			if(err)
				console.log(err);
			callback(err, newName);
		});
	},
	dropCollection: function(collectionName, callback){
		fs.unlink(this.dataPath + '/' + collectionName + '.db', callback);
	}
};

TuleCollection.prototype = {
	find: function(query, options, callback){
		var me = this;
		if(this.db.then){
			return this.db.then(function(){
				me.find(query, options,callback);
			});
		}
		var q = query || {},
			o = false,
			c = false
		;

		if(typeof options == 'function')
			c = options;
		else{
			o = options;
			c = callback;
		}

		var find = this.db.find(q);
		if(o){
			if(o.sort)
				find.sort(o.sort);
			if(o.skip)
				find.skip(o.skip);
			if(o.limit)
				find.limit(o.limit);
		}
		find.exec(c);
	},
	findOne: function(query, options, callback){
		var me = this;
		if(this.db.then){
			return this.db.then(function(){
				me.findOne(query, options, callback);
			});
		}
		var q = query || {},
			o = {},
			c = false
		;

		if(typeof options == 'function')
			c = options;
		else{
			o = options;
			c = callback;
		}

		o.limit = 1;
		this.find(q, o, function(err, docs){
			if(err)
				return c(err);
			if(docs.length)
				return c(null, docs[0]);
			return c(null, null);
		});
	},

	insert: function(docs, callback){
		var me = this;
		if(this.db.then){
			return this.db.then(function(){
				me.insert(docs, callback);
			});
		}
		this.db.insert(docs, callback);
	},
	update: function(){
		var me = this,
			args = arguments
		;
		if(this.db.then){
			return this.db.then(function(){
				me.update.apply(me, args);
			});
		}
		this.db.update.apply(this.db, args);
	},
	save: function(doc, callback){
		if(doc._id)
			this.update({_id: doc._id}, doc, callback);
		else
			this.insert(doc, callback);
	},
	remove: function(query, callback){
		var me = this;
		if(this.db.then){
			return this.db.then(function(){
				me.remove(query, callback);
			});
		}
		this.db.remove(query, {multi:1},callback);
	},
	count: function(query, callback){
		var me = this;
		if(this.db.then){
			return this.db.then(function(){
				me.count(query, callback);
			});
		}
		if(typeof query === 'function'){
			callback = query;
			query = {};
		}
		else if(typeof query === 'undefined')
			query = {};

		this.db.count(query, callback);
	}
};

module.exports = {
	init: function(options){
		return when.resolve(new NedbDriver(options));
	}
};
