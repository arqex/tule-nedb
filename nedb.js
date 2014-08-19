'use strict';

module.exports = {
	init: function(hooks){

		// Add the driver to the list of available ones.
		hooks.addFilter('db:drivers', function(drivers){
			drivers.nedb = __dirname + '/nedbDriver';
			return drivers;
		});
	}
};