{
  "name": "default",
  "type": "mysql",
  "host": "${RS_HOST}",
  "port": ${RS_PORT},
  "username": "${RS_USERNAME}",
  "password": "${RS_PASSWORD}",
  "database": "${RS_DATABASE}",
  "synchronize": true,
  "logging": false,
  "entities": [
    "dist/entity/*.js"
  ],
  "subscribers": [
    "dist/subscriber/*.js"
  ],
  "migrations": [
    "dist/migration/*.js"
  ],
  "cli": {
    "entitiesDir": "src/entity",
    "migrationsDir": "src/migration",
    "subscribersDir": "src/subscriber"
  }
}
