import sendRes from '../../../libs/send-res-with-module-map'
import db from '../../../libs/db'

export default async (req, res) => {

  if (req.method === 'GET') {
    console.time('Get note from Noria')

    const notes = await db.execute(
      `SELECT
         id, title, updated_at, body, created_by
       FROM notes
       WHERE title ILIKE ?
       ORDER BY id ASC`,
      [`%${req.query.q}%`]
    )

    console.timeEnd('Get note from Noria')
    return res.send(JSON.stringify(notes))
  }

  if (req.method === 'POST') {
    console.time('Create note in Noria')

    const { insertId } = await db.execute(
      `INSERT INTO notes (title, body, updated_at)
       VALUES (?, ?, ?)`,
      [
        (req.body.title || '').slice(0, 255),
        (req.body.body || '').slice(0, 2048),
        db.formatDate(new Date()),
      ]
    )

    console.timeEnd('Create note in Noria')

    return sendRes(req, res, insertId)
  }

  return res.send('Method not allowed.')
}
