import mysql from 'mysql2'
import { promisify } from 'util'
import { DateTime } from 'luxon'
import { escape } from 'sqlstring'

const conn = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD,
})

export default {
  query: promisify(conn.query).bind(conn),
  execute: promisify(conn.execute).bind(conn),

  formatDate(date) {
    return DateTime.fromJSDate(date).toFormat('yyyy-MM-dd HH:mm:ss')
  },

  escape,
}
