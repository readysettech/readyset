var db = {
	"dialect": "mysql",
	"host": process.env.RS_HOST,
	"port": process.env.RS_PORT,
	"username": process.env.RS_USERNAME,
	"password": process.env.RS_PASSWORD,
	"database": process.env.RS_DATABASE
};

module.exports = {
	"development": db,
	"test": db,
	"production": db
};

