import React from 'react'

import TextWithMarkdown from './TextWithMarkdown'

export default function NotePreview({ body }) {
  return (
    <div className="note-preview">
      <TextWithMarkdown text={body} />
    </div>
  )
}
