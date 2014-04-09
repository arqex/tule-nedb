var nedb = require('nedb'),
	when = require('when'),
	config = require('config'),
	fs = require('fs')
;

var NedbDriver = function(){},
	collections = {},
	TuleCollection = function(collectionName){
		var me = this,
			deferred = when.defer(),
			collection = collections[collectionName]
		;

		this.collectionName = collectionName;

		if(collection)
			this.db = collection;
		else{
			this.db = deferred.promise;
			collection = new nedb({
				filename: config.nedb.dataPath + '/' + collectionName + '.db',
				autoload: true,
				onload: function(err){
					if(err){
						console.log(err);
						deferred.reject(err);
					}
					else{
						me.db = collection;
						collections[collectionName] = collection;
						deferred.resolve(collection);
					}
				}
			});

		}
	}
;

NedbDriver.prototype = {
	collection: function(collectionName){
		return new TuleCollection(collectionName);
	},
	createCollection: function(collectionName, callback){
		fs.open(config.nedb.dataPath + '/' + collectionName + '.db', 'a', function(err, fd){
			if(err)
				return callback(err);

			fs.close(fd, function(err){
				callback(err, new TuleCollection(collectionName));
			});
		});
	},
	getCollectionNames: function(callback){
		var collectionNames = [];
		fs.readdir(config.nedb.dataPath, function(err, files){
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
		var oldPath = config.nedb.dataPath + '/' + oldName + '.db',
			newPath = config.nedb.dataPath + '/' + newName + '.db'
		;

		fs.rename(oldPath, newPath, function(err){
			if(err)
				console.log(err);
			callback(err, newName);
		});
		/*
		fs.renameSync(oldPath, newPath);
		callback(null, newName);*/
	},
	dropCollection: function(collectionName, callback){
		fs.unlink(config.nedb.dataPath + '/' + collectionName + '.db', callback);
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
}

module.exports = {
	init: function(){
		return when.resolve(new NedbDriver());
	}
}
