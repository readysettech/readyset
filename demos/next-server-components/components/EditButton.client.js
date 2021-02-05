import React, { unstable_useTransition } from 'react'

import { useLocation } from './LocationContext.client'

export default function EditButton({ noteId, disabled, title, children }) {
  const [, setLocation] = useLocation()
  const [startTransition, isPending] = unstable_useTransition()
  const isDraft = noteId == null
  return (
    <button
      className={[
        'edit-button',
        isDraft ? 'edit-button--solid' : 'edit-button--outline',
      ].join(' ')}
      disabled={isPending || disabled}
      title={title}
      onClick={() => {
        if (isDraft) {
          // hide the sidebar
          const sidebarToggle = document.getElementById('sidebar-toggle')
          if (sidebarToggle) {
            sidebarToggle.checked = true
          }
        }
        startTransition(() => {
          setLocation(loc => ({
            selectedId: noteId,
            isEditing: true,
            searchText: loc.searchText,
          }))
        })
      }}
      role="menuitem"
    >
      {children}
    </button>
  )
}
