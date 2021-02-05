import sendRes from '../../../libs/send-res-with-module-map'
import db from '../../../libs/db'

export default async (req, res) => {
  const id = +req.query.id

  console.time('Get note from Noria')
  const result = await db.execute(
    `SELECT
         id, title, updated_at, body, created_by
       FROM notes
       WHERE id = ?`,
    [id]
  )
  const note = result[0]
  console.timeEnd('Get note from Noria')

  if (req.method === 'GET') {
    return res.send(JSON.stringify(note))
  }

  if (req.method === 'DELETE') {
    console.time('Delete note in Noria')
    await db.execute('DELETE FROM notes WHERE id = ?', [id])
    console.timeEnd('Delete note in Noria')

    return sendRes(req, res, null)
  }

  if (req.method === 'PUT') {
    console.time('Update note in Noria')
    await db.execute(
      `UPDATE notes SET
         title = ${db.escape((req.body.title || '').slice(0, 255))},
         body = ${db.escape((req.body.body || '').slice(0, 2048))},
         updated_at = ${db.escape(db.formatDate(new Date()))}
       WHERE id = ${id}`
    )
    console.timeEnd('Update note in Noria')

    return sendRes(req, res, null)
  }

  return res.send('Method not allowed.')
}
