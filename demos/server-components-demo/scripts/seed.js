/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

'use strict';

const fs = require('fs');
const path = require('path');
const {Pool} = require('pg');
const {readdir, unlink, writeFile} = require('fs/promises');
const startOfYear = require('date-fns/startOfYear');
const credentials = require('../credentials');

const NOTES_PATH = './notes';
const pool = new Pool(credentials);

const now = new Date();
const startOfThisYear = startOfYear(now);
// Thanks, https://stackoverflow.com/a/9035732
function randomDateBetween(start, end) {
  return new Date(
    start.getTime() + Math.random() * (end.getTime() - start.getTime())
  );
}

const dropTableStatement = 'DROP TABLE IF EXISTS notes;';
const createTableStatement = `CREATE TABLE notes (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  title TEXT,
  body TEXT
);`;
const insertNoteStatement = `INSERT INTO notes(title, body, created_at, updated_at)
  VALUES ($1, $2, $3, $3)
  RETURNING *`;
const seedData = [
  [
    'Meeting Notes',
    'This is an example note. It contains **Markdown**!',
    randomDateBetween(startOfThisYear, now),
  ],
  [
    'Make a thing',
    `It's very easy to make some words **bold** and other words *italic* with
Markdown. You can even [link to React's website!](https://www.reactjs.org).`,
    randomDateBetween(startOfThisYear, now),
  ],
  [
    'A note with a very long title because sometimes you need more words',
    `You can write all kinds of [amazing](https://en.wikipedia.org/wiki/The_Amazing)
notes in this app! These note live on the server in the \`notes\` folder.

![This app is powered by React](https://upload.wikimedia.org/wikipedia/commons/thumb/1/18/React_Native_Logo.png/800px-React_Native_Logo.png)`,
    randomDateBetween(startOfThisYear, now),
  ],
  ['I wrote this note today', 'It was an excellent note.', now],
];

async function seed() {
  await pool.query(dropTableStatement);
  await pool.query(createTableStatement);
  const res = await Promise.all(
    seedData.map((row) => pool.query(insertNoteStatement, row))
  );

  const oldNotes = await readdir(path.resolve(NOTES_PATH));
  await Promise.all(
    oldNotes
      .filter((filename) => filename.endsWith('.md'))
      .map((filename) => unlink(path.resolve(NOTES_PATH, filename)))
  );

  await Promise.all(
    res.map(({rows}) => {
      const id = rows[0].id;
      const content = rows[0].body;
      const data = new Uint8Array(Buffer.from(content));
      return writeFile(path.resolve(NOTES_PATH, `${id}.md`), data, (err) => {
        if (err) {
          throw err;
        }
      });
    })
  );
}

seed();
