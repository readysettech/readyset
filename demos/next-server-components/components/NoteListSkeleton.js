import React from 'react'

export default function NoteListSkeleton() {
  return (
    <div>
      <ul className="notes-list skeleton-container">
        <li className="v-stack">
          <div
            className="sidebar-note-list-item skeleton"
            style={{ height: '5em' }}
          />
        </li>
        <li className="v-stack">
          <div
            className="sidebar-note-list-item skeleton"
            style={{ height: '5em' }}
          />
        </li>
        <li className="v-stack">
          <div
            className="sidebar-note-list-item skeleton"
            style={{ height: '5em' }}
          />
        </li>
      </ul>
    </div>
  )
}
