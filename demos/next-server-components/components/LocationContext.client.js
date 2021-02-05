import { createContext, useContext } from 'react'

export const LocationContext = createContext()
export function useLocation() {
  return useContext(LocationContext)
}
